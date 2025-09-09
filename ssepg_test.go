package ssepg_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/subnetmarco/ssepg"
)

// Test database URL - uses testcontainers or local postgres
func getTestDSN(_ *testing.T) string {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}
	return dsn
}

func setupTestService(t *testing.T) (*ssepg.Service, *http.ServeMux) {
	cfg := ssepg.DefaultConfig()
	cfg.DSN = getTestDSN(t)
	cfg.KeepAlive = 1 * time.Second // Faster for tests
	cfg.GracefulDrain = 1 * time.Second

	svc, err := ssepg.New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to create service: %v", err)
	}

	mux := http.NewServeMux()
	svc.Attach(mux)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = svc.Close(ctx)
		// Give a small grace period for cleanup
		time.Sleep(100 * time.Millisecond)
	})

	return svc, mux
}

func TestBasicPublishSubscribe(t *testing.T) {
	svc, mux := setupTestService(t)
	server := httptest.NewServer(mux)
	defer server.Close()

	// Test data
	testTopic := "test-topic"
	testData := map[string]interface{}{
		"message":   "hello world",
		"timestamp": time.Now().Unix(),
	}

	// Start SSE subscription
	resp, err := http.Get(server.URL + "/topics/" + testTopic + "/events")
	if err != nil {
		t.Fatalf("Failed to connect to SSE: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Errorf("Expected Content-Type text/event-stream, got %s", resp.Header.Get("Content-Type"))
	}

	// Channel to receive messages
	messages := make(chan map[string]interface{}, 10)
	go func() {
		const messageEvent = "message"
		scanner := NewSSEScanner(resp.Body)
		for scanner.Scan() {
			event := scanner.Event()
			if event.Event == messageEvent && event.Data != "" {
				var data map[string]interface{}
				if jsonErr := json.Unmarshal([]byte(event.Data), &data); jsonErr == nil {
					messages <- data
				}
			}
		}
	}()

	// Give SSE connection time to establish
	time.Sleep(100 * time.Millisecond)

	// Publish message via HTTP
	payload, _ := json.Marshal(map[string]interface{}{"data": testData})
	publishResp, err := http.Post(
		server.URL+"/topics/"+testTopic+"/events",
		"application/json",
		bytes.NewBuffer(payload),
	)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}
	defer func() { _ = publishResp.Body.Close() }()

	if publishResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(publishResp.Body)
		t.Fatalf("Publish failed with status %d: %s", publishResp.StatusCode, body)
	}

	// Wait for message
	select {
	case receivedData := <-messages:
		if receivedData["message"] != testData["message"] {
			t.Errorf("Expected message %v, got %v", testData["message"], receivedData["message"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	// Test programmatic publish
	err = svc.Publish(context.Background(), testTopic, json.RawMessage(`{"programmatic": true}`))
	if err != nil {
		t.Fatalf("Programmatic publish failed: %v", err)
	}

	// Wait for programmatic message
	select {
	case receivedData := <-messages:
		if receivedData["programmatic"] != true {
			t.Errorf("Expected programmatic message, got %v", receivedData)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for programmatic message")
	}
}

func TestTopicIsolation(t *testing.T) {
	_, mux := setupTestService(t)
	server := httptest.NewServer(mux)
	defer server.Close()

	topic1, topic2 := "topic-1", "topic-2"

	// Subscribe to both topics
	resp1, err := http.Get(server.URL + "/topics/" + topic1 + "/events")
	if err != nil {
		t.Fatalf("Failed to connect to topic1 SSE: %v", err)
	}
	defer func() { _ = resp1.Body.Close() }()

	resp2, err := http.Get(server.URL + "/topics/" + topic2 + "/events")
	if err != nil {
		t.Fatalf("Failed to connect to topic2 SSE: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()

	// Channels for messages
	messages1 := make(chan map[string]interface{}, 10)
	messages2 := make(chan map[string]interface{}, 10)

	// Start readers
	go readSSEMessages(resp1.Body, messages1)
	go readSSEMessages(resp2.Body, messages2)

	time.Sleep(100 * time.Millisecond)

	// Publish to topic1
	payload1, _ := json.Marshal(map[string]interface{}{"data": map[string]string{"topic": "1"}})
	_, _ = http.Post(server.URL+"/topics/"+topic1+"/events", "application/json", bytes.NewBuffer(payload1))

	// Publish to topic2
	payload2, _ := json.Marshal(map[string]interface{}{"data": map[string]string{"topic": "2"}})
	_, _ = http.Post(server.URL+"/topics/"+topic2+"/events", "application/json", bytes.NewBuffer(payload2))

	// Verify topic1 only gets its message
	select {
	case msg := <-messages1:
		if msg["topic"] != "1" {
			t.Errorf("Topic1 received wrong message: %v", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Topic1 didn't receive message")
	}

	// Verify topic2 only gets its message
	select {
	case msg := <-messages2:
		if msg["topic"] != "2" {
			t.Errorf("Topic2 received wrong message: %v", msg)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Topic2 didn't receive message")
	}

	// Verify no cross-contamination (should timeout)
	select {
	case msg := <-messages1:
		t.Errorf("Topic1 received unexpected message: %v", msg)
	case <-time.After(1 * time.Second):
		// Expected timeout
	}

	select {
	case msg := <-messages2:
		t.Errorf("Topic2 received unexpected message: %v", msg)
	case <-time.After(1 * time.Second):
		// Expected timeout
	}
}

func TestMultipleSubscribers(t *testing.T) {
	_, mux := setupTestService(t)
	server := httptest.NewServer(mux)
	defer server.Close()

	topic := "multi-sub-topic"
	numSubscribers := 5

	// Create multiple subscribers
	var messageChannels []chan map[string]interface{}

	for i := 0; i < numSubscribers; i++ {
		resp, err := http.Get(server.URL + "/topics/" + topic + "/events")
		if err != nil {
			t.Fatalf("Failed to connect subscriber %d: %v", i, err)
		}
		defer func() { _ = resp.Body.Close() }()

		messages := make(chan map[string]interface{}, 10)
		go readSSEMessages(resp.Body, messages)

		messageChannels = append(messageChannels, messages)
	}

	time.Sleep(200 * time.Millisecond)

	// Publish a message
	testData := map[string]interface{}{"subscriber_test": true, "id": 12345}
	payload, _ := json.Marshal(map[string]interface{}{"data": testData})
	_, _ = http.Post(server.URL+"/topics/"+topic+"/events", "application/json", bytes.NewBuffer(payload))

	// Verify all subscribers receive the message
	var wg sync.WaitGroup
	wg.Add(numSubscribers)

	for i := 0; i < numSubscribers; i++ {
		go func(subIndex int, msgChan chan map[string]interface{}) {
			defer wg.Done()
			select {
			case msg := <-msgChan:
				if msg["subscriber_test"] != true || msg["id"] != float64(12345) {
					t.Errorf("Subscriber %d received wrong message: %v", subIndex, msg)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("Subscriber %d didn't receive message", subIndex)
			}
		}(i, messageChannels[i])
	}

	wg.Wait()
}

func TestInvalidRequests(t *testing.T) {
	_, mux := setupTestService(t)
	server := httptest.NewServer(mux)
	defer server.Close()

	tests := []struct {
		name           string
		method         string
		path           string
		body           string
		contentType    string
		expectedStatus int
	}{
		{
			name:           "Invalid topic name",
			method:         "POST",
			path:           "/topics/invalid@topic/events",
			body:           `{"data": {"test": true}}`,
			contentType:    "application/json",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Empty topic name",
			method:         "POST",
			path:           "/topics//events",
			body:           `{"data": {"test": true}}`,
			contentType:    "application/json",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "Invalid JSON",
			method:         "POST",
			path:           "/topics/valid-topic/events",
			body:           `{"data": invalid json}`,
			contentType:    "application/json",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Missing data field",
			method:         "POST",
			path:           "/topics/valid-topic/events",
			body:           `{"notdata": true}`,
			contentType:    "application/json",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Empty data field",
			method:         "POST",
			path:           "/topics/valid-topic/events",
			body:           `{"data": null}`,
			contentType:    "application/json",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Wrong method for subscribe",
			method:         "POST",
			path:           "/topics/valid-topic/events",
			body:           "",
			contentType:    "",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body io.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			}

			req, err := http.NewRequest(tt.method, server.URL+tt.path, body)
			if err != nil {
				t.Fatalf("Failed to create request: %v", err)
			}

			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("Request failed: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != tt.expectedStatus {
				bodyBytes, _ := io.ReadAll(resp.Body)
				t.Errorf("Expected status %d, got %d. Body: %s", tt.expectedStatus, resp.StatusCode, bodyBytes)
			}
		})
	}
}

func TestHealthEndpoint(t *testing.T) {
	svc, mux := setupTestService(t)
	server := httptest.NewServer(mux)
	defer server.Close()

	// Publish some messages to generate metrics
	testData := json.RawMessage(`{"test": true}`)
	_ = svc.Publish(context.Background(), "health-test", testData)

	resp, err := http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	if resp.Header.Get("Content-Type") != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", resp.Header.Get("Content-Type"))
	}

	var health map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("Failed to decode health response: %v", err)
	}

	// Check required fields
	if health["status"] != "ok" {
		t.Errorf("Expected status 'ok', got %v", health["status"])
	}

	if health["totals"] == nil {
		t.Error("Missing totals in health response")
	}

	if health["topics"] == nil {
		t.Error("Missing topics in health response")
	}
}

func TestSSEHeartbeat(t *testing.T) {
	_, mux := setupTestService(t)
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/topics/heartbeat-test/events")
	if err != nil {
		t.Fatalf("Failed to connect to SSE: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Read raw data to look for heartbeat comments
	buffer := make([]byte, 1024)
	heartbeatReceived := false
	timeout := time.After(3 * time.Second)

	for !heartbeatReceived {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for heartbeat")
		default:
			n, err := resp.Body.Read(buffer)
			if err != nil && err != io.EOF {
				t.Fatalf("Error reading SSE: %v", err)
			}
			if n > 0 {
				data := string(buffer[:n])
				if strings.Contains(data, "keep-alive") || strings.Contains(data, "listening") {
					heartbeatReceived = true
				}
			}
		}
	}
}

func TestGzipCompression(t *testing.T) {
	_, mux := setupTestService(t)
	server := httptest.NewServer(mux)
	defer server.Close()

	req, err := http.NewRequest("GET", server.URL+"/topics/gzip-test/events", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to connect to SSE: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Errorf("Expected gzip encoding, got %s", resp.Header.Get("Content-Encoding"))
	}
}

// Helper functions

func readSSEMessages(body io.Reader, messages chan<- map[string]interface{}) {
	const messageEvent = "message"
	scanner := NewSSEScanner(body)
	for scanner.Scan() {
		event := scanner.Event()
		if event.Event == messageEvent && event.Data != "" {
			var data map[string]interface{}
			if jsonErr := json.Unmarshal([]byte(event.Data), &data); jsonErr == nil {
				messages <- data
			}
		}
	}
}

// SSE Scanner for parsing Server-Sent Events
type SSEEvent struct {
	Event string
	Data  string
	ID    string
}

type SSEScanner struct {
	scanner *bytes.Buffer
	reader  io.Reader
	event   SSEEvent
}

func NewSSEScanner(r io.Reader) *SSEScanner {
	return &SSEScanner{
		scanner: new(bytes.Buffer),
		reader:  r,
	}
}

func (s *SSEScanner) Scan() bool {
	// Read more data
	buf := make([]byte, 1024)
	n, err := s.reader.Read(buf)
	if err != nil && err != io.EOF {
		return false
	}
	if n == 0 && err == io.EOF {
		return false
	}

	s.scanner.Write(buf[:n])

	// Look for complete events (ending with \n\n)
	data := s.scanner.String()
	eventEnd := strings.Index(data, "\n\n")
	if eventEnd == -1 {
		return s.Scan() // Need more data
	}

	// Extract event
	eventData := data[:eventEnd]
	s.scanner.Reset()
	s.scanner.WriteString(data[eventEnd+2:]) // Keep remaining data

	// Parse event
	s.event = SSEEvent{}
	lines := strings.Split(eventData, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, ":") {
			continue // Comment
		}
		if strings.HasPrefix(line, "event: ") {
			s.event.Event = strings.TrimPrefix(line, "event: ")
		} else if strings.HasPrefix(line, "data: ") {
			if s.event.Data != "" {
				s.event.Data += "\n"
			}
			s.event.Data += strings.TrimPrefix(line, "data: ")
		} else if strings.HasPrefix(line, "id: ") {
			s.event.ID = strings.TrimPrefix(line, "id: ")
		}
	}

	return true
}

func (s *SSEScanner) Event() SSEEvent {
	return s.event
}
