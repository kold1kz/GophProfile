{{- define "gophprofile.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "gophprofile.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "gophprofile.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{- define "gophprofile.labels" -}}
helm.sh/chart: {{ include "gophprofile.chart" . }}
app.kubernetes.io/name: {{ include "gophprofile.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "gophprofile.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gophprofile.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "gophprofile.serverSelectorLabels" -}}
{{ include "gophprofile.selectorLabels" . }}
app.kubernetes.io/component: server
{{- end -}}

{{- define "gophprofile.workerSelectorLabels" -}}
{{ include "gophprofile.selectorLabels" . }}
app.kubernetes.io/component: worker
{{- end -}}

{{- define "gophprofile.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "gophprofile.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "gophprofile.secretName" -}}
{{- if .Values.secrets.name -}}
{{- .Values.secrets.name -}}
{{- else -}}
{{- printf "%s-secrets" (include "gophprofile.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "gophprofile.commonEnv" -}}
- name: PUBLIC_BASE_URL
  valueFrom:
    configMapKeyRef:
      name: {{ include "gophprofile.fullname" . }}-config
      key: PUBLIC_BASE_URL
- name: S3_ENDPOINT
  valueFrom:
    configMapKeyRef:
      name: {{ include "gophprofile.fullname" . }}-config
      key: S3_ENDPOINT
- name: S3_BUCKET
  valueFrom:
    configMapKeyRef:
      name: {{ include "gophprofile.fullname" . }}-config
      key: S3_BUCKET
- name: S3_USE_SSL
  valueFrom:
    configMapKeyRef:
      name: {{ include "gophprofile.fullname" . }}-config
      key: S3_USE_SSL
- name: RABBITMQ_EXCHANGE
  valueFrom:
    configMapKeyRef:
      name: {{ include "gophprofile.fullname" . }}-config
      key: RABBITMQ_EXCHANGE
- name: RABBITMQ_QUEUE
  valueFrom:
    configMapKeyRef:
      name: {{ include "gophprofile.fullname" . }}-config
      key: RABBITMQ_QUEUE
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  valueFrom:
    configMapKeyRef:
      name: {{ include "gophprofile.fullname" . }}-config
      key: OTEL_EXPORTER_OTLP_ENDPOINT
- name: MAX_FILE_SIZE
  valueFrom:
    configMapKeyRef:
      name: {{ include "gophprofile.fullname" . }}-config
      key: MAX_FILE_SIZE
- name: SHUTDOWN_DELAY
  valueFrom:
    configMapKeyRef:
      name: {{ include "gophprofile.fullname" . }}-config
      key: SHUTDOWN_DELAY
- name: RATE_LIMIT_RPS
  valueFrom:
    configMapKeyRef:
      name: {{ include "gophprofile.fullname" . }}-config
      key: RATE_LIMIT_RPS
- name: RATE_LIMIT_BURST
  valueFrom:
    configMapKeyRef:
      name: {{ include "gophprofile.fullname" . }}-config
      key: RATE_LIMIT_BURST
- name: DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "gophprofile.secretName" . }}
      key: DATABASE_URL
- name: S3_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "gophprofile.secretName" . }}
      key: S3_ACCESS_KEY
- name: S3_SECRET_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "gophprofile.secretName" . }}
      key: S3_SECRET_KEY
- name: RABBITMQ_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "gophprofile.secretName" . }}
      key: RABBITMQ_URL
{{- end -}}
