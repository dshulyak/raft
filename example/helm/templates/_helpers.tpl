{{/*
fully qualified app name
*/}}
{{- define "kv.fullname" -}}
{{- printf " %s" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kv.chart" -}}
{{- printf " %s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
common labels
*/}}
{{- define "kv.labels" -}}
app.kubernetes.io/name: {{ include "kv.fullname" . }}
helm.sh/chart: {{ include "kv.chart" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
full image path
*/}}
{{- define "kv.image" -}}
{{- printf " %s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}