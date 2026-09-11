//go:build ignore

// Generate cursors.json with the public bstream cursor format and upstream opaque
// library. Run in a Go module containing github.com/streamingfast/opaque at
// v0.0.0-20210811180740-0c01d37ea308. No provider or user credentials are involved.
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/streamingfast/opaque"
)

func main() {
	all := map[string]map[int]string{}
	for _, step := range []int{1, 17} {
		items := map[int]string{}
		for number := 100; number <= 110; number++ {
			items[number] = opaque.EncodeString(fmt.Sprintf("c1:%d:%d:%064x:%d:%064x", step, number, number, number, number))
		}
		all[fmt.Sprint(step)] = items
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(all); err != nil { panic(err) }
}
