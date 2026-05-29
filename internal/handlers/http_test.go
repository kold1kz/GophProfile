package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gophprofile/internal/domain"
	"gophprofile/internal/services"
	"gophprofile/pkg/imaging"

	"github.com/stretchr/testify/require"
)

func TestWriteImageSetsContentHashETag(t *testing.T) {
	avatar := domain.Avatar{
		ID:        "avatar-1",
		MimeType:  "image/jpeg",
		UpdatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}
	body := []byte("image-data")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/avatars/avatar-1", nil)
	rec := httptest.NewRecorder()

	require.NoError(t, writeImage(rec, req, avatar, bytes.NewReader(body), "image/jpeg"))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "image/jpeg", rec.Header().Get("Content-Type"))
	require.NotEmpty(t, rec.Header().Get("ETag"))
	require.Equal(t, body, rec.Body.Bytes())
}

func TestWriteImageHonorsIfNoneMatch(t *testing.T) {
	avatar := domain.Avatar{ID: "avatar-1", MimeType: "image/jpeg", UpdatedAt: time.Now()}
	first := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/avatars/avatar-1", nil)
	require.NoError(t, writeImage(first, req, avatar, bytes.NewBufferString("image-data"), "image/jpeg"))

	second := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/avatars/avatar-1", nil)
	req.Header.Set("If-None-Match", first.Header().Get("ETag"))
	require.NoError(t, writeImage(second, req, avatar, bytes.NewBufferString("image-data"), "image/jpeg"))

	require.Equal(t, http.StatusNotModified, second.Code)
	require.Empty(t, second.Body.String())
}

func TestWriteServiceErrorDoesNotExposeInternalDetails(t *testing.T) {
	handler := &AvatarHandler{}
	rec := httptest.NewRecorder()

	handler.writeServiceError(context.Background(), rec, errors.New("database password leaked in error"))

	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.NotContains(t, rec.Body.String(), "database password")
	require.Contains(t, rec.Body.String(), "Internal server error")
}

func TestAvatarHandlerRoutes(t *testing.T) {
	avatar := domain.Avatar{
		ID:               "avatar-1",
		UserID:           "user-1",
		FileName:         "avatar.jpg",
		MimeType:         "image/jpeg",
		ProcessingStatus: domain.ProcessingStatusCompleted,
		CreatedAt:        time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		UpdatedAt:        time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}

	t.Run("health ok", func(t *testing.T) {
		router := NewAvatarHandler(&fakeAvatarService{}, fakeHealth{"db": "ok"}, 1024).Routes()
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), `"status":"ok"`)
	})

	t.Run("health degraded", func(t *testing.T) {
		router := NewAvatarHandler(&fakeAvatarService{}, fakeHealth{"db": "error"}, 1024).Routes()
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		require.Contains(t, rec.Body.String(), `"status":"degraded"`)
	})

	t.Run("liveness does not check dependencies", func(t *testing.T) {
		router := NewAvatarHandler(&fakeAvatarService{}, fakeHealth{"db": "error"}, 1024).Routes()
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/live", nil))

		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), `"status":"ok"`)
	})

	t.Run("rate limit protects api and skips probes", func(t *testing.T) {
		router := NewAvatarHandler(
			&fakeAvatarService{},
			fakeHealth{"db": "ok"},
			1024,
			WithRateLimit(RateLimitConfig{RequestsPerSecond: 1, Burst: 1}),
		).Routes()

		first := httptest.NewRecorder()
		router.ServeHTTP(first, multipartUploadRequest(t, http.MethodPost, "/api/v1/avatars", "file", "avatar.jpg", []byte("data"), nil))
		require.Equal(t, http.StatusBadRequest, first.Code)

		second := httptest.NewRecorder()
		router.ServeHTTP(second, multipartUploadRequest(t, http.MethodPost, "/api/v1/avatars", "file", "avatar.jpg", []byte("data"), nil))
		require.Equal(t, http.StatusTooManyRequests, second.Code)

		probe := httptest.NewRecorder()
		router.ServeHTTP(probe, httptest.NewRequest(http.MethodGet, "/live", nil))
		require.Equal(t, http.StatusOK, probe.Code)
	})

	t.Run("api upload requires user header", func(t *testing.T) {
		router := NewAvatarHandler(&fakeAvatarService{}, fakeHealth{}, 1024).Routes()
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, multipartUploadRequest(t, http.MethodPost, "/api/v1/avatars", "file", "avatar.jpg", []byte("data"), nil))

		require.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("api upload succeeds", func(t *testing.T) {
		svc := &fakeAvatarService{uploadAvatar: avatar}
		router := NewAvatarHandler(svc, fakeHealth{}, 1024).Routes()
		req := multipartUploadRequest(t, http.MethodPost, "/api/v1/avatars", "file", "avatar.jpg", []byte("data"), nil)
		req.Header.Set("X-User-ID", "user-1")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusCreated, rec.Code)
		require.Equal(t, "user-1", svc.uploadInput.UserID)
		require.Contains(t, rec.Body.String(), `"id":"avatar-1"`)
	})

	t.Run("api upload maps service error", func(t *testing.T) {
		svc := &fakeAvatarService{uploadErr: services.ErrFileTooLarge}
		router := NewAvatarHandler(svc, fakeHealth{}, 512).Routes()
		req := multipartUploadRequest(t, http.MethodPost, "/api/v1/avatars", "file", "avatar.jpg", []byte("data"), nil)
		req.Header.Set("X-User-ID", "user-1")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	})

	t.Run("api upload accepts image field fallback", func(t *testing.T) {
		svc := &fakeAvatarService{uploadAvatar: avatar}
		handler := NewAvatarHandler(svc, fakeHealth{}, 1024)
		rec := httptest.NewRecorder()
		req := multipartUploadRequest(t, http.MethodPost, "/api/v1/avatars", "image", "avatar.jpg", []byte("data"), nil)

		_, err := handler.parseAndUpload(rec, req, "user-1")

		require.NoError(t, err)
		require.Equal(t, "avatar.jpg", svc.uploadInput.FileName)
	})

	t.Run("parse upload rejects invalid multipart", func(t *testing.T) {
		handler := NewAvatarHandler(&fakeAvatarService{}, fakeHealth{}, 1024)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", bytes.NewBufferString("bad"))
		req.Header.Set("Content-Type", "multipart/form-data; boundary=broken")

		_, err := handler.parseAndUpload(httptest.NewRecorder(), req, "user-1")

		require.ErrorIs(t, err, services.ErrFileTooLarge)
	})

	t.Run("get avatar writes image", func(t *testing.T) {
		svc := &fakeAvatarService{getAvatar: avatar, body: []byte("image")}
		router := NewAvatarHandler(svc, fakeHealth{}, 1024).Routes()
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/avatars/avatar-1", nil))

		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, []byte("image"), rec.Body.Bytes())
	})

	t.Run("get user avatar writes image", func(t *testing.T) {
		svc := &fakeAvatarService{latestAvatar: avatar, body: []byte("image")}
		router := NewAvatarHandler(svc, fakeHealth{}, 1024).Routes()
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/users/user-1/avatar", nil))

		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, []byte("image"), rec.Body.Bytes())
	})

	t.Run("metadata", func(t *testing.T) {
		svc := &fakeAvatarService{metadataAvatar: avatar}
		router := NewAvatarHandler(svc, fakeHealth{}, 1024).Routes()
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/avatars/avatar-1/metadata", nil))

		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), `"id":"avatar-1"`)
	})

	t.Run("list user avatars", func(t *testing.T) {
		svc := &fakeAvatarService{listAvatars: []domain.Avatar{avatar}}
		router := NewAvatarHandler(svc, fakeHealth{}, 1024).Routes()
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/users/user-1/avatars", nil))

		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), `"id":"avatar-1"`)
	})

	t.Run("delete avatar requires header", func(t *testing.T) {
		router := NewAvatarHandler(&fakeAvatarService{}, fakeHealth{}, 1024).Routes()
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/avatars/avatar-1", nil))

		require.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("delete avatar succeeds", func(t *testing.T) {
		svc := &fakeAvatarService{}
		router := NewAvatarHandler(svc, fakeHealth{}, 1024).Routes()
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/avatars/avatar-1", nil)
		req.Header.Set("X-User-ID", "user-1")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusNoContent, rec.Code)
		require.Equal(t, "avatar-1", svc.deletedID)
		require.Equal(t, "user-1", svc.deletedUserID)
	})

	t.Run("delete latest avatar requires user header", func(t *testing.T) {
		router := NewAvatarHandler(&fakeAvatarService{}, fakeHealth{}, 1024).Routes()
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/users/user-1/avatar", nil))

		require.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("delete latest avatar checks owner", func(t *testing.T) {
		router := NewAvatarHandler(&fakeAvatarService{}, fakeHealth{}, 1024).Routes()
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/users/user-1/avatar", nil)
		req.Header.Set("X-User-ID", "other")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("delete selected user avatar", func(t *testing.T) {
		svc := &fakeAvatarService{metadataAvatar: avatar}
		router := NewAvatarHandler(svc, fakeHealth{}, 1024).Routes()
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/users/user-1/avatar?avatar_id=avatar-1", nil)
		req.Header.Set("X-User-ID", "user-1")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusNoContent, rec.Code)
		require.Equal(t, "avatar-1", svc.deletedID)
	})

	t.Run("delete latest user avatar", func(t *testing.T) {
		svc := &fakeAvatarService{metadataErr: domain.ErrNotFound, latestMetadataAvatar: avatar}
		router := NewAvatarHandler(svc, fakeHealth{}, 1024).Routes()
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/users/user-1/avatar", nil)
		req.Header.Set("X-User-ID", "user-1")
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusNoContent, rec.Code)
		require.Equal(t, "avatar-1", svc.deletedID)
	})

	t.Run("web upload requires user", func(t *testing.T) {
		router := NewAvatarHandler(&fakeAvatarService{}, fakeHealth{}, 1024).Routes()
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, multipartUploadRequest(t, http.MethodPost, "/web/upload", "file", "avatar.jpg", []byte("data"), nil))

		require.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("web upload redirects to gallery", func(t *testing.T) {
		svc := &fakeAvatarService{uploadAvatar: avatar}
		router := NewAvatarHandler(svc, fakeHealth{}, 1024).Routes()
		req := multipartUploadRequest(t, http.MethodPost, "/web/upload", "file", "avatar.jpg", []byte("data"), map[string]string{"user_id": "user-1"})
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		require.Equal(t, http.StatusFound, rec.Code)
		require.Equal(t, "/web/gallery/user-1", rec.Header().Get("Location"))
	})

	t.Run("web upload get redirects home", func(t *testing.T) {
		router := NewAvatarHandler(&fakeAvatarService{}, fakeHealth{}, 1024).Routes()
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/web/upload", nil))

		require.Equal(t, http.StatusFound, rec.Code)
		require.Equal(t, "/", rec.Header().Get("Location"))
	})

	t.Run("web pages are routed", func(t *testing.T) {
		router := NewAvatarHandler(&fakeAvatarService{}, fakeHealth{}, 1024).Routes()

		for _, path := range []string{"/", "/web/gallery/user-1"} {
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			require.NotZero(t, rec.Code)
		}
	})
}

func TestWriteServiceErrorMappings(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code int
		body string
	}{
		{name: "unsupported format", err: imaging.ErrUnsupportedFormat, code: http.StatusBadRequest, body: "Invalid file format"},
		{name: "forbidden", err: domain.ErrForbidden, code: http.StatusForbidden, body: "Forbidden"},
		{name: "not found", err: domain.ErrNotFound, code: http.StatusNotFound, body: "Avatar not found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &AvatarHandler{}
			rec := httptest.NewRecorder()

			handler.writeServiceError(context.Background(), rec, tt.err)

			require.Equal(t, tt.code, rec.Code)
			require.Contains(t, rec.Body.String(), tt.body)
		})
	}
}

func TestWriteImageUsesAvatarMimeAndReturnsReadError(t *testing.T) {
	avatar := domain.Avatar{ID: "avatar-1", MimeType: "image/png", UpdatedAt: time.Now()}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/avatars/avatar-1", nil)
	rec := httptest.NewRecorder()

	require.NoError(t, writeImage(rec, req, avatar, bytes.NewBufferString("image"), ""))
	require.Equal(t, "image/png", rec.Header().Get("Content-Type"))

	err := writeImage(httptest.NewRecorder(), req, avatar, errReader{}, "image/jpeg")
	require.Error(t, err)
}

func TestWriteErrorAndAvatarResponse(t *testing.T) {
	rec := httptest.NewRecorder()
	writeError(rec, http.StatusTeapot, "short", "details")

	require.Equal(t, http.StatusTeapot, rec.Code)
	require.Contains(t, rec.Body.String(), `"details":"details"`)

	resp := avatarResponse(domain.Avatar{ID: "avatar-1", UserID: "user-1", ProcessingStatus: domain.ProcessingStatusPending})
	require.Equal(t, "/api/v1/avatars/avatar-1", resp["url"])
	require.Equal(t, "user-1", resp["user_id"])
}

type fakeAvatarService struct {
	uploadAvatar         domain.Avatar
	uploadErr            error
	uploadInput          services.UploadInput
	getAvatar            domain.Avatar
	latestAvatar         domain.Avatar
	metadataAvatar       domain.Avatar
	latestMetadataAvatar domain.Avatar
	listAvatars          []domain.Avatar
	body                 []byte
	getErr               error
	latestErr            error
	metadataErr          error
	latestMetadataErr    error
	listErr              error
	deleteErr            error
	deletedID            string
	deletedUserID        string
}

func (s *fakeAvatarService) Upload(_ context.Context, in services.UploadInput) (domain.Avatar, error) {
	s.uploadInput = in
	return s.uploadAvatar, s.uploadErr
}

func (s *fakeAvatarService) Get(context.Context, string) (domain.Avatar, io.ReadCloser, string, error) {
	if s.getErr != nil {
		return domain.Avatar{}, nil, "", s.getErr
	}
	return s.getAvatar, io.NopCloser(bytes.NewReader(s.body)), "image/jpeg", nil
}

func (s *fakeAvatarService) GetLatestForUser(context.Context, string) (domain.Avatar, io.ReadCloser, string, error) {
	if s.latestErr != nil {
		return domain.Avatar{}, nil, "", s.latestErr
	}
	return s.latestAvatar, io.NopCloser(bytes.NewReader(s.body)), "image/jpeg", nil
}

func (s *fakeAvatarService) Metadata(context.Context, string) (domain.Avatar, error) {
	return s.metadataAvatar, s.metadataErr
}

func (s *fakeAvatarService) LatestMetadata(context.Context, string) (domain.Avatar, error) {
	return s.latestMetadataAvatar, s.latestMetadataErr
}

func (s *fakeAvatarService) List(context.Context, string) ([]domain.Avatar, error) {
	return s.listAvatars, s.listErr
}

func (s *fakeAvatarService) Delete(_ context.Context, id, userID string) error {
	s.deletedID = id
	s.deletedUserID = userID
	return s.deleteErr
}

type fakeHealth map[string]string

func (h fakeHealth) Check(*http.Request) map[string]string {
	return h
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func multipartUploadRequest(t *testing.T, method, path, field, filename string, data []byte, fields map[string]string) *http.Request {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for name, value := range fields {
		err := writer.WriteField(name, value)
		require.NoError(t, err)
	}
	part, err := writer.CreateFormFile(field, filename)
	require.NoError(t, err)
	_, err = part.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	req := httptest.NewRequest(method, path, &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return req
}

func decodeJSONBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var payload map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
	return payload
}
