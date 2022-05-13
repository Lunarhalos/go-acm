package filter

import (
	"fmt"
	"net/http"
)

func CORS() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.Method == http.MethodOptions {
				fmt.Println("cors:", req.Method, req.RequestURI)
				w.Header().Set("Access-Control-Allow-Origin", "*")
				w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "authorization, origin, content-type, accept")
				w.Header().Set("Allow", "HEAD,GET,POST,PUT,PATCH,DELETE,OPTIONS")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				return
			}
			next.ServeHTTP(w, req)
		})
	}
}
