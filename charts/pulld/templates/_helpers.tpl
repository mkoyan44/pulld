{{/*
Expand the name of the chart.
*/}}
{{- define "pulld.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "pulld.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "pulld.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "pulld.labels" -}}
helm.sh/chart: {{ include "pulld.chart" . }}
{{ include "pulld.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels.
*/}}
{{- define "pulld.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pulld.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Create the service account name to use.
*/}}
{{- define "pulld.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "pulld.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Resolve the image tag.
*/}}
{{- define "pulld.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
Resolve the cache claim name.
*/}}
{{- define "pulld.cacheClaimName" -}}
{{- if .Values.persistence.existingClaim -}}
{{- .Values.persistence.existingClaim -}}
{{- else -}}
{{- include "pulld.fullname" . -}}
{{- end -}}
{{- end -}}
