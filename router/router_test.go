package router

import (
	"testing"

	"telegram/config"
)

func TestAuthorizedSecret(t *testing.T) {
	tests := []struct {
		name   string
		secret string
		got    string
		want   bool
	}{
		{"未設定則放行", "", "anything", true},
		{"未設定且空 header 也放行", "", "", true},
		{"相符放行", "s3cret", "s3cret", true},
		{"不符擋下", "s3cret", "wrong", false},
		{"缺 header 擋下", "s3cret", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config.WebhookSecret = tt.secret
			if got := authorizedSecret(tt.got); got != tt.want {
				t.Errorf("authorizedSecret(%q) with secret %q = %v, want %v",
					tt.got, tt.secret, got, tt.want)
			}
		})
	}
}
