package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	url := os.Args[1]
	key := os.Args[2]
	h := http.Header{}
	h.Set("Authorization", "Bearer "+key)
	h.Set("session_id", "ws-audit-1")
	h.Set("session-id", "ws-audit-1")
	h.Set("thread-id", "ws-audit-1")
	h.Set("User-Agent", "codex_cli_rs/0.172.0")
	h.Set("originator", "codex_cli_rs")

	conn, resp, err := websocket.DefaultDialer.Dial(url, h)
	if err != nil {
		fmt.Printf("DIAL_ERR: %v\n", err)
		if resp != nil {
			fmt.Printf("STATUS: %d\n", resp.StatusCode)
		}
		return
	}
	defer conn.Close()
	fmt.Printf("HANDSHAKE_OK status=%d\n", resp.StatusCode)

	create := map[string]any{
		"type":   "response.create",
		"model":  "gpt-5.6-sol",
		"stream": true,
		"input":  []any{map[string]any{"role": "user", "content": "say hi"}},
	}
	if err := conn.WriteJSON(create); err != nil {
		fmt.Printf("WRITE_ERR: %v\n", err)
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	for i := 0; i < 3; i++ {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			fmt.Printf("READ_END: %v\n", err)
			return
		}
		var m map[string]any
		_ = json.Unmarshal(msg, &m)
		b, _ := json.Marshal(m)
		s := string(b)
		if len(s) > 220 {
			s = s[:220]
		}
		fmt.Printf("EVENT: %s\n", s)
	}
}
