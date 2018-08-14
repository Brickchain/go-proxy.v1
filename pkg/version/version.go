//go:generate sh -c "sed -i \"s/const Version = \\\".*\\\"/const Version = \\\"`echo $VERSION`\\\"/\" version.go"

package version

// Version number
var Version = ""
