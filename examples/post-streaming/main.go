package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

func main() {
	// Test 1: Normal POST (backward compatibility)
	fmt.Println("Test 1: Normal POST request (JSON response)...")
	testNormalPost()
	
	time.Sleep(1 * time.Second)
	
	// Test 2: Streaming POST
	fmt.Println("\nTest 2: POST request with streaming response...")
	testStreamingPost()
}

func testNormalPost() {
	payload := map[string]interface{}{
		"data": map[string]string{
			"message": "Test normal POST",
			"time":    time.Now().Format(time.RFC3339),
		},
	}
	
	body, _ := json.Marshal(payload)
	resp, err := http.Post("http://localhost:8080/topics/test/events", "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}
	defer resp.Body.Close()
	
	fmt.Printf("Response Status: %s\n", resp.Status)
	fmt.Printf("Content-Type: %s\n", resp.Header.Get("Content-Type"))
	
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	fmt.Printf("Response Body: %+v\n", result)
}

func testStreamingPost() {
	payload := map[string]interface{}{
		"data": map[string]string{
			"message": "Test streaming POST",
			"time":    time.Now().Format(time.RFC3339),
		},
	}
	
	body, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", "http://localhost:8080/topics/test/events", bytes.NewBuffer(body))
	if err != nil {
		log.Printf("Error creating request: %v", err)
		return
	}
	
	// Set Accept header to request streaming response
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Content-Type", "application/json")
	
	client := &http.Client{
		Timeout: 10 * time.Second, // Set timeout for the test
	}
	
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error: %v", err)
		return
	}
	defer resp.Body.Close()
	
	fmt.Printf("Response Status: %s\n", resp.Status)
	fmt.Printf("Content-Type: %s\n", resp.Header.Get("Content-Type"))
	
	// Read SSE stream
	scanner := bufio.NewScanner(resp.Body)
	eventCount := 0
	maxEvents := 3 // Read first 3 events then close
	
	fmt.Println("Streaming events:")
	for scanner.Scan() && eventCount < maxEvents {
		line := scanner.Text()
		if line != "" {
			fmt.Printf("  %s\n", line)
			if line[:5] == "data:" {
				eventCount++
			}
		}
	}
	
	if err := scanner.Err(); err != nil && err.Error() != "context deadline exceeded" {
		log.Printf("Error reading stream: %v", err)
	}
}