package storage

import (
	"context"
	"io"
	"net/url"
	"time"

	"gophprofile/internal/observability"
	"gophprofile/pkg/circuitbreaker"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// MinIOStorage работает с MinIO как с S3-совместимым объектным хранилищем.
type MinIOStorage struct {
	client        *minio.Client
	presignClient *minio.Client
	bucket        string
	breaker       *circuitbreaker.CircuitBreaker
}

// NewMinIO создает клиент MinIO и гарантирует, что нужный bucket уже существует.
func NewMinIO(ctx context.Context, endpoint, publicBaseURL, accessKey, secretKey, bucket string, useSSL bool) (*MinIOStorage, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
		Region: "us-east-1",
	})
	if err != nil {
		return nil, err
	}
	presignClient := client
	if publicBaseURL != "" {
		publicEndpoint, publicUseSSL, err := minioEndpointFromURL(publicBaseURL)
		if err != nil {
			return nil, err
		}
		presignClient, err = minio.New(publicEndpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
			Secure: publicUseSSL,
			Region: "us-east-1",
		})
		if err != nil {
			return nil, err
		}
	}
	storage := &MinIOStorage{
		client:        client,
		presignClient: presignClient,
		bucket:        bucket,
		breaker:       circuitbreaker.New("s3"),
	}
	if err := storage.ensureBucket(ctx); err != nil {
		return nil, err
	}
	return storage, nil
}

func minioEndpointFromURL(rawURL string) (string, bool, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", false, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", false, url.InvalidHostError(rawURL)
	}
	if u.Host == "" {
		return "", false, url.InvalidHostError(rawURL)
	}
	return u.Host, u.Scheme == "https", nil
}

func (s *MinIOStorage) ensureBucket(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if err := s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{}); err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.Code == "BucketAlreadyOwnedByYou" || resp.Code == "BucketAlreadyExists" {
			return nil
		}
		return err
	}
	return nil
}

// Upload сохраняет объект в bucket под переданным ключом.
func (s *MinIOStorage) Upload(ctx context.Context, key, contentType string, size int64, body io.Reader) error {
	return circuitbreaker.ExecuteVoid(s.breaker, func() error {
		return s.upload(ctx, key, contentType, size, body)
	})
}

func (s *MinIOStorage) upload(ctx context.Context, key, contentType string, size int64, body io.Reader) error {
	ctx, span := observability.Tracer().Start(ctx, "s3.upload_object")
	defer span.End()
	span.SetAttributes(attribute.String("s3.key", key), attribute.String("content_type", contentType), attribute.Int64("size", size))
	_, err := s.client.PutObject(ctx, s.bucket, key, body, size, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

// Download открывает объект на чтение и возвращает его content type.
func (s *MinIOStorage) Download(ctx context.Context, key string) (io.ReadCloser, string, error) {
	result, err := circuitbreaker.Execute(s.breaker, func() (struct {
		body        io.ReadCloser
		contentType string
	}, error) {
		body, contentType, err := s.download(ctx, key)
		return struct {
			body        io.ReadCloser
			contentType string
		}{body: body, contentType: contentType}, err
	})
	return result.body, result.contentType, err
}

func (s *MinIOStorage) download(ctx context.Context, key string) (io.ReadCloser, string, error) {
	ctx, span := observability.Tracer().Start(ctx, "s3.download_object")
	defer span.End()
	span.SetAttributes(attribute.String("s3.key", key))
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, "", err
	}
	stat, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, "", err
	}
	return obj, stat.ContentType, nil
}

// Delete удаляет объект из bucket; для несуществующего ключа MinIO обычно не считает это ошибкой.
func (s *MinIOStorage) Delete(ctx context.Context, key string) error {
	return circuitbreaker.ExecuteVoid(s.breaker, func() error {
		return s.delete(ctx, key)
	})
}

func (s *MinIOStorage) delete(ctx context.Context, key string) error {
	ctx, span := observability.Tracer().Start(ctx, "s3.delete_object")
	defer span.End()
	span.SetAttributes(attribute.String("s3.key", key))
	err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

// PresignedGetURL создает временную ссылку на объект, чтобы ее можно было вернуть клиенту.
func (s *MinIOStorage) PresignedGetURL(ctx context.Context, key string) (string, error) {
	return circuitbreaker.Execute(s.breaker, func() (string, error) {
		return s.presignedGetURL(ctx, key)
	})
}

func (s *MinIOStorage) presignedGetURL(ctx context.Context, key string) (string, error) {
	_, span := observability.Tracer().Start(ctx, "s3.presign_get_object")
	defer span.End()
	span.SetAttributes(attribute.String("s3.key", key))
	u, err := s.presignClient.PresignedGetObject(ctx, s.bucket, key, 24*time.Hour, url.Values{})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}
	return u.String(), nil
}

// Ping проверяет доступность bucket в MinIO.
func (s *MinIOStorage) Ping(ctx context.Context) error {
	return circuitbreaker.ExecuteVoid(s.breaker, func() error {
		_, err := s.client.BucketExists(ctx, s.bucket)
		return err
	})
}
