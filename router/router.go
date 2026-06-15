package router

import (
	"fmt"
	"net/http"
)

func New() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /hello", hello)

	return mux
}

func hello(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "hello world")
}
