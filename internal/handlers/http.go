package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"gophprofile/internal/domain"
	"gophprofile/internal/observability"
	"gophprofile/internal/services"
	"gophprofile/pkg/imaging"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// AvatarHandler принимает HTTP-запросы и переводит их в вызовы AvatarService.
type AvatarHandler struct {
	service     AvatarService
	health      HealthChecker
	maxFileSize int64
}

// AvatarService описывает методы бизнес-логики, которые нужны HTTP-слою.
type AvatarService interface {
	Upload(ctx context.Context, in services.UploadInput) (domain.Avatar, error)
	Get(ctx context.Context, id string) (domain.Avatar, io.ReadCloser, string, error)
	GetLatestForUser(ctx context.Context, userID string) (domain.Avatar, io.ReadCloser, string, error)
	Metadata(ctx context.Context, id string) (domain.Avatar, error)
	LatestMetadata(ctx context.Context, userID string) (domain.Avatar, error)
	List(ctx context.Context, userID string) ([]domain.Avatar, error)
	Delete(ctx context.Context, id, userID string) error
}

// HealthChecker описывает объект, который умеет проверить состояние зависимостей приложения.
type HealthChecker interface {
	Check(r *http.Request) map[string]string
}

// NewAvatarHandler собирает HTTP-обработчик с сервисом аватаров и health-checker'ом.
func NewAvatarHandler(service AvatarService, health HealthChecker, maxFileSize int64) *AvatarHandler {
	return &AvatarHandler{service: service, health: health, maxFileSize: maxFileSize}
}

// Routes строит HTTP-маршруты API и подключает готовый static frontend.
func (h *AvatarHandler) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(observability.HTTPMiddleware)
	r.Get("/health", h.healthCheck)
	r.Handle("/metrics", promhttp.Handler())

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/avatars", h.uploadAvatar)
		r.Get("/avatars/{avatar_id}", h.getAvatar)
		r.Get("/avatars/{avatar_id}/metadata", h.getMetadata)
		r.Delete("/avatars/{avatar_id}", h.deleteAvatar)
		r.Get("/users/{user_id}/avatar", h.getUserAvatar)
		r.Delete("/users/{user_id}/avatar", h.deleteUserAvatar)
		r.Get("/users/{user_id}/avatars", h.listUserAvatars)
	})

	r.Get("/", h.webIndex)
	r.Get("/web/upload", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/", http.StatusFound)
	})
	r.Post("/web/upload", h.webUpload)
	r.Get("/web/gallery/{user_id}", h.webGallery)
	r.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.Dir("web/static"))))
	return otelhttp.NewHandler(r, "http.server")
}

func (h *AvatarHandler) uploadAvatar(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(r.Header.Get("X-User-ID"))
	if userID == "" {
		writeError(w, http.StatusBadRequest, "X-User-ID header is required", "")
		return
	}
	avatar, err := h.parseAndUpload(w, r, userID)
	if err != nil {
		h.writeServiceError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusCreated, avatarResponse(avatar))
}

func (h *AvatarHandler) getAvatar(w http.ResponseWriter, r *http.Request) {
	avatar, body, contentType, err := h.service.Get(r.Context(), chi.URLParam(r, "avatar_id"))
	if err != nil {
		h.writeServiceError(r.Context(), w, err)
		return
	}
	defer body.Close()
	if err := writeImage(w, r, avatar, body, contentType); err != nil {
		slog.LogAttrs(r.Context(), slog.LevelError, "write avatar image", append(observability.LogAttrs(r.Context()),
			slog.String("avatar_id", avatar.ID),
			slog.Any("error", err),
		)...)
	}
}

func (h *AvatarHandler) getUserAvatar(w http.ResponseWriter, r *http.Request) {
	avatar, body, contentType, err := h.service.GetLatestForUser(r.Context(), chi.URLParam(r, "user_id"))
	if err != nil {
		h.writeServiceError(r.Context(), w, err)
		return
	}
	defer body.Close()
	if err := writeImage(w, r, avatar, body, contentType); err != nil {
		slog.LogAttrs(r.Context(), slog.LevelError, "write latest avatar image", append(observability.LogAttrs(r.Context()),
			slog.String("avatar_id", avatar.ID),
			slog.Any("error", err),
		)...)
	}
}

func (h *AvatarHandler) getMetadata(w http.ResponseWriter, r *http.Request) {
	avatar, err := h.service.Metadata(r.Context(), chi.URLParam(r, "avatar_id"))
	if err != nil {
		h.writeServiceError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, avatar)
}

func (h *AvatarHandler) listUserAvatars(w http.ResponseWriter, r *http.Request) {
	avatars, err := h.service.List(r.Context(), chi.URLParam(r, "user_id"))
	if err != nil {
		h.writeServiceError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, avatars)
}

func (h *AvatarHandler) deleteAvatar(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(r.Header.Get("X-User-ID"))
	if userID == "" {
		writeError(w, http.StatusBadRequest, "X-User-ID header is required", "")
		return
	}
	if err := h.service.Delete(r.Context(), chi.URLParam(r, "avatar_id"), userID); err != nil {
		h.writeServiceError(r.Context(), w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *AvatarHandler) deleteUserAvatar(w http.ResponseWriter, r *http.Request) {
	userID := chi.URLParam(r, "user_id")
	headerUserID := strings.TrimSpace(r.Header.Get("X-User-ID"))
	if headerUserID == "" {
		writeError(w, http.StatusBadRequest, "X-User-ID header is required", "")
		return
	}
	if headerUserID != userID {
		writeError(w, http.StatusForbidden, "Forbidden", "You can only delete your own avatars")
		return
	}
	avatarID := strings.TrimSpace(r.URL.Query().Get("avatar_id"))
	if avatarID != "" {
		avatar, err := h.service.Metadata(r.Context(), avatarID)
		if err != nil {
			h.writeServiceError(r.Context(), w, err)
			return
		}
		if err := h.service.Delete(r.Context(), avatar.ID, userID); err != nil {
			h.writeServiceError(r.Context(), w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	latest, err := h.service.LatestMetadata(r.Context(), userID)
	if err != nil {
		h.writeServiceError(r.Context(), w, err)
		return
	}
	if err := h.service.Delete(r.Context(), latest.ID, userID); err != nil {
		h.writeServiceError(r.Context(), w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *AvatarHandler) healthCheck(w http.ResponseWriter, r *http.Request) {
	components := h.health.Check(r)
	status := "ok"
	code := http.StatusOK
	for _, value := range components {
		if value != "ok" {
			status = "degraded"
			code = http.StatusServiceUnavailable
			break
		}
	}
	writeJSON(w, code, map[string]any{"status": status, "components": components})
}

func (h *AvatarHandler) webIndex(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "web/static/index.html")
}

func (h *AvatarHandler) webUpload(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(r.FormValue("user_id"))
	if userID == "" {
		userID = strings.TrimSpace(r.Header.Get("X-User-ID"))
	}
	if userID == "" {
		writeError(w, http.StatusBadRequest, "user_id is required", "")
		return
	}
	avatar, err := h.parseAndUpload(w, r, userID)
	if err != nil {
		h.writeServiceError(r.Context(), w, err)
		return
	}
	http.Redirect(w, r, "/web/gallery/"+avatar.UserID, http.StatusFound)
}

func (h *AvatarHandler) webGallery(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "web/static/index.html")
}

func (h *AvatarHandler) parseAndUpload(w http.ResponseWriter, r *http.Request, userID string) (domain.Avatar, error) {
	r.Body = http.MaxBytesReader(w, r.Body, h.maxFileSize+1024)
	if err := r.ParseMultipartForm(h.maxFileSize); err != nil {
		return domain.Avatar{}, services.ErrFileTooLarge
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		file, header, err = r.FormFile("image")
	}
	if err != nil {
		return domain.Avatar{}, err
	}
	defer file.Close()
	return h.service.Upload(r.Context(), services.UploadInput{
		UserID:   userID,
		FileName: header.Filename,
		Size:     header.Size,
		Reader:   file,
	})
}

func (h *AvatarHandler) writeServiceError(ctx context.Context, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, services.ErrFileTooLarge):
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": "File too large", "max_size": h.maxFileSize})
	case errors.Is(err, imaging.ErrUnsupportedFormat):
		writeError(w, http.StatusBadRequest, "Invalid file format", "Supported formats: jpeg, png, webp")
	case errors.Is(err, domain.ErrForbidden):
		writeError(w, http.StatusForbidden, "Forbidden", "You can only delete your own avatars")
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "Avatar not found", "")
	default:
		slog.LogAttrs(ctx, slog.LevelError, "internal handler error", append(observability.LogAttrs(ctx),
			slog.Any("error", err),
		)...)
		writeError(w, http.StatusInternalServerError, "Internal server error", "")
	}
}

func writeImage(w http.ResponseWriter, r *http.Request, avatar domain.Avatar, body io.Reader, contentType string) error {
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(data)
	etag := `"` + hex.EncodeToString(hash[:]) + `"`
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return nil
	}
	if contentType == "" {
		contentType = avatar.MimeType
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "max-age=86400")
	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", avatar.UpdatedAt.UTC().Format(http.TimeFormat))
	_, err = w.Write(data)
	return err
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, code int, message, details string) {
	payload := map[string]any{"error": message}
	if details != "" {
		payload["details"] = details
	}
	writeJSON(w, code, payload)
}

func avatarResponse(avatar domain.Avatar) map[string]any {
	return map[string]any{
		"id":         avatar.ID,
		"user_id":    avatar.UserID,
		"url":        "/api/v1/avatars/" + avatar.ID,
		"status":     avatar.ProcessingStatus,
		"created_at": avatar.CreatedAt,
	}
}
