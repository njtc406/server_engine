package cluster

// 动态节点配置缓存
var dyNodeCache *NodeInfo

func AddNodeConfCache(n *NodeInfo) {
	dyNodeCache = n
}

func DelNodeConfCache() {
	dyNodeCache = nil
}
