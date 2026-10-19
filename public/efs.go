package public

import "embed"

//go:embed "index.html" "main.js"
var Files embed.FS
