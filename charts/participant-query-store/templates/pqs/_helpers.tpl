{{- define "pqs.secretEnvVarName" -}}
PQS_SECRET_{{ . }}
{{- end -}}

{{- define "pqs.serviceAccountName" -}}
{{- default .Release.Namespace .Values.serviceAccount.name }}
{{- end }}
