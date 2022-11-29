package cluster

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/njtc406/server_engine/log"
	"github.com/njtc406/server_engine/rpc"
	"os"
	"strings"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type NodeInfoList struct {
	MasterDiscoveryNode []NodeInfo //用于服务发现Node
	NodeList            []NodeInfo
}

func (cls *Cluster) ReadClusterConfig(filepath string) (*NodeInfoList, error) {
	c := &NodeInfoList{}
	d, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(d, c)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (cls *Cluster) readServiceConfig(filepath string) (interface{}, map[string]interface{}, map[int]map[string]interface{}, error) {
	c := map[string]interface{}{}
	//读取配置
	d, err := os.ReadFile(filepath)
	if err != nil {
		return nil, nil, nil, err
	}
	err = json.Unmarshal(d, &c)
	if err != nil {
		return nil, nil, nil, err
	}

	GlobalCfg, ok := c["Global"]
	serviceConfig := map[string]interface{}{}
	serviceCfg, ok := c["Service"]
	if ok == true {
		serviceConfig = serviceCfg.(map[string]interface{})
	}

	mapNodeService := map[int]map[string]interface{}{}
	nodeServiceCfg, ok := c["NodeService"]
	if ok == true {
		nodeServiceList := nodeServiceCfg.([]interface{})
		for _, v := range nodeServiceList {
			serviceCfg := v.(map[string]interface{})
			nodeId, ok := serviceCfg["NodeId"]
			if ok == false {
				log.SFatal("NodeService list not find nodeId field")
			}
			mapNodeService[int(nodeId.(float64))] = serviceCfg
		}
	}
	return GlobalCfg, serviceConfig, mapNodeService, nil
}

func (cls *Cluster) readLocalClusterConfig(nodeId int) ([]NodeInfo, []NodeInfo, error) {
	var nodeInfoList []NodeInfo
	var masterDiscoverNodeList []NodeInfo
	clusterCfgPath := strings.TrimRight(configDir, "/") + "/cluster/cluster.json"
	localNodeInfoList, err := cls.ReadClusterConfig(clusterCfgPath)
	if err != nil {
		return nil, nil, fmt.Errorf("read cluster config error:%+v", err)
	}
	masterDiscoverNodeList = append(masterDiscoverNodeList, localNodeInfoList.MasterDiscoveryNode...)
	for _, nodeInfo := range localNodeInfoList.NodeList {
		if nodeInfo.NodeId == nodeId || nodeId == 0 {
			nodeInfoList = append(nodeInfoList, nodeInfo)
		}
	}

	if nodeId != 0 && (len(nodeInfoList) != 1) {
		if dyNodeCache == nil {
			return nil, nil, fmt.Errorf("%d configurations were found for the configuration with node ID %d!", len(nodeInfoList), nodeId)
		}
		//动态节点配置
		nodeInfoList = append(nodeInfoList, *dyNodeCache)
	}

	for i, _ := range nodeInfoList {
		for j, s := range nodeInfoList[i].ServiceList {
			//私有结点不加入到Public服务列表中
			if strings.HasPrefix(s, "_") == false && nodeInfoList[i].Private == false {
				nodeInfoList[i].PublicServiceList = append(nodeInfoList[i].PublicServiceList, strings.TrimLeft(s, "_"))
			} else {
				nodeInfoList[i].ServiceList[j] = strings.TrimLeft(s, "_")
			}
		}
	}

	return masterDiscoverNodeList, nodeInfoList, nil
}

func (cls *Cluster) readLocalService(localNodeId int) error {
	clusterCfgPath := strings.TrimRight(configDir, "/") + "/cluster/service.json"

	var globalCfg interface{}
	publicService := map[string]interface{}{}
	nodeService := map[string]interface{}{}

	currGlobalCfg, serviceConfig, mapNodeService, err := cls.readServiceConfig(clusterCfgPath)
	if err != nil {
		return fmt.Errorf("read service config error: %s", err)
	}

	if currGlobalCfg != nil {
		//不允许重复的配置global配置
		if globalCfg != nil {
			return fmt.Errorf("[Global] does not allow repeated service config")
		}
		globalCfg = currGlobalCfg
	}

	//保存公共配置
	for _, s := range cls.localNodeInfo.ServiceList {
		for {
			//取公共服务配置
			pubCfg, ok := serviceConfig[s]
			if ok == true {
				if _, publicOk := publicService[s]; publicOk == true {
					return fmt.Errorf("public service [%s] does not allow repeated configuration.", s)
				}
				publicService[s] = pubCfg
			}

			//取指定结点配置的服务
			nodeServiceCfg, ok := mapNodeService[localNodeId]
			if ok == false {
				break
			}
			nodeCfg, ok := nodeServiceCfg[s]
			if ok == false {
				break
			}

			if _, nodeOK := nodeService[s]; nodeOK == true {
				return fmt.Errorf("NodeService NodeId[%d] Service[%s] does not allow repeated configuration.", cls.localNodeInfo.NodeId, s)
			}
			nodeService[s] = nodeCfg
			break
		}
	}

	//组合所有的配置
	for _, s := range cls.localNodeInfo.ServiceList {
		//先从NodeService中找
		var serviceCfg interface{}
		var ok bool
		serviceCfg, ok = nodeService[s]
		if ok == true {
			cls.localServiceCfg[s] = serviceCfg
			continue
		}

		//如果找不到从PublicService中找
		serviceCfg, ok = publicService[s]
		if ok == true {
			cls.localServiceCfg[s] = serviceCfg
		}
	}
	cls.globalCfg = globalCfg

	return nil
}

func (cls *Cluster) parseLocalCfg() {
	cls.mapIdNode[cls.localNodeInfo.NodeId] = cls.localNodeInfo

	for _, sName := range cls.localNodeInfo.ServiceList {
		if _, ok := cls.mapServiceNode[sName]; ok == false {
			cls.mapServiceNode[sName] = make(map[int]struct{})
		}

		cls.mapServiceNode[sName][cls.localNodeInfo.NodeId] = struct{}{}
	}
}

func (cls *Cluster) checkDiscoveryNodeList(discoverMasterNode []NodeInfo) bool {
	for i := 0; i < len(discoverMasterNode)-1; i++ {
		for j := i + 1; j < len(discoverMasterNode); j++ {
			if discoverMasterNode[i].NodeId == discoverMasterNode[j].NodeId ||
				discoverMasterNode[i].ListenAddr == discoverMasterNode[j].ListenAddr {
				return false
			}
		}
	}

	return true
}

func (cls *Cluster) InitCfg(localNodeId int) error {
	cls.localServiceCfg = map[string]interface{}{}
	cls.mapRpc = map[int]NodeRpcInfo{}
	cls.mapIdNode = map[int]NodeInfo{}
	cls.mapServiceNode = map[string]map[int]struct{}{}

	//加载本地结点的NodeList配置
	discoveryNode, nodeInfoList, err := cls.readLocalClusterConfig(localNodeId)
	if err != nil {
		return err
	}
	cls.localNodeInfo = nodeInfoList[0]
	if cls.checkDiscoveryNodeList(discoveryNode) == false {
		return fmt.Errorf("DiscoveryNode config is error!")
	}
	cls.masterDiscoveryNodeList = discoveryNode

	//读取本地服务配置
	err = cls.readLocalService(localNodeId)
	if err != nil {
		return err
	}

	//本地配置服务加到全局map信息中
	cls.parseLocalCfg()
	return nil
}

func (cls *Cluster) IsConfigService(serviceName string) bool {
	cls.locker.RLock()
	defer cls.locker.RUnlock()
	mapNode, ok := cls.mapServiceNode[serviceName]
	if ok == false {
		return false
	}

	_, ok = mapNode[cls.localNodeInfo.NodeId]
	return ok
}

func (cls *Cluster) GetNodeIdByService(serviceName string, rpcClientList []*rpc.Client, bAll bool) (error, int) {
	cls.locker.RLock()
	defer cls.locker.RUnlock()
	mapNodeId, ok := cls.mapServiceNode[serviceName]
	count := 0
	if ok == true {
		for nodeId, _ := range mapNodeId {
			pClient := GetCluster().getRpcClient(nodeId)
			if pClient == nil || (bAll == false && pClient.IsConnected() == false) {
				continue
			}
			rpcClientList[count] = pClient
			count++
			if count >= cap(rpcClientList) {
				break
			}
		}
	}

	return nil, count
}

func (cls *Cluster) GetServiceCfg(serviceName string) interface{} {
	serviceCfg, ok := cls.localServiceCfg[serviceName]
	if ok == false {
		return nil
	}

	return serviceCfg
}
