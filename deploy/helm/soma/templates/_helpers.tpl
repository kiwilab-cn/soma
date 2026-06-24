{{/* Chart name. */}}
{{- define "soma.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified base name. */}}
{{- define "soma.fullname" -}}
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

{{/* Per-role resource names. */}}
{{- define "soma.gatewayName" -}}{{ printf "%s-gateway" (include "soma.fullname" .) | trunc 63 | trimSuffix "-" }}{{- end -}}
{{- define "soma.metaName" -}}{{ printf "%s-meta" (include "soma.fullname" .) | trunc 63 | trimSuffix "-" }}{{- end -}}
{{- define "soma.storageName" -}}{{ printf "%s-storage" (include "soma.fullname" .) | trunc 63 | trimSuffix "-" }}{{- end -}}
{{- define "soma.metaHeadless" -}}{{ printf "%s-meta-headless" (include "soma.fullname" .) | trunc 63 | trimSuffix "-" }}{{- end -}}
{{- define "soma.storageHeadless" -}}{{ printf "%s-storage-headless" (include "soma.fullname" .) | trunc 63 | trimSuffix "-" }}{{- end -}}

{{/* The Secret holding the access credentials. */}}
{{- define "soma.secretName" -}}
{{- if .Values.credentials.existingSecret -}}{{ .Values.credentials.existingSecret }}{{- else -}}{{ include "soma.fullname" . }}{{- end -}}
{{- end -}}

{{/* Common labels (component supplied by the caller as `.component`). */}}
{{- define "soma.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "soma.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* Selector labels for a role (component supplied as `.component`). */}}
{{- define "soma.selectorLabels" -}}
app.kubernetes.io/name: {{ include "soma.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* The metadata endpoint URL (meta StatefulSet pod 0). */}}
{{- define "soma.metaEndpoint" -}}
http://{{ include "soma.metaName" . }}-0.{{ include "soma.metaHeadless" . }}.{{ .Release.Namespace }}.svc:{{ .Values.ports.meta }}
{{- end -}}
