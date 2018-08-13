//go:generate sh -c "sed -i \"s/const Version = \\\".*\\\"/const Version = \\\"`git describe --always --abbrev=12`\\\"/\" version.go"

package version

// Version git version number
const Version = ""
