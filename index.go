package cluster_default

import "github.com/chefsgo/chef"

func Driver() chef.ClusterDriver {
	return &defaultClusterDriver{}
}

func init() {
	chef.Register("default", Driver())
}
