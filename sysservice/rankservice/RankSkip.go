package rankservice

import (
	"fmt"
	"github.com/njtc406/server_engine/rpc"
	"github.com/njtc406/server_engine/util/algorithms/skip"
	"time"
)

type RankSkip struct {
	rankId         uint64               //排行榜ID
	isDes          bool                 //是否为降序 true：降序 false：升序
	skipList       *skip.SkipList       //跳表
	mapRankData    map[uint64]*RankData //排行数据map
	maxLen         uint64               //排行数据长度
	expireMs       time.Duration        //有效时间
	rankModule     IRankModule
	rankDataExpire rankDataHeap
}

const MaxPickExpireNum = 2
const (
	RankDataNone   RankDataChangeType = 0
	RankDataAdd    RankDataChangeType = 1 //数据插入
	RankDataUpdate RankDataChangeType = 2 //数据更新
	RankDataDelete RankDataChangeType = 3 //数据删除
)

// NewRankSkip 创建排行榜
func NewRankSkip(isDes bool, level interface{}, maxLen uint64, expireMs time.Duration) *RankSkip {
	rs := &RankSkip{}

	rs.isDes = isDes
	rs.skipList = skip.New(level)
	rs.mapRankData = make(map[uint64]*RankData, 10240)
	rs.maxLen = maxLen
	rs.expireMs = expireMs
	rs.rankDataExpire.Init(int32(maxLen), expireMs)

	return rs
}

func (rs *RankSkip) pickExpireKey() {
	if rs.expireMs == 0 {
		return
	}

	for i := 1; i < MaxPickExpireNum; i++ {
		key := rs.rankDataExpire.PopExpireKey()
		if key == 0 {
			return
		}

		rs.DeleteRankData([]uint64{key})
	}
}

func (rs *RankSkip) SetupRankModule(rankModule IRankModule) {
	rs.rankModule = rankModule
}

// GetRankID 获取排行榜Id
func (rs *RankSkip) GetRankID() uint64 {
	return rs.rankId
}

// GetRankLen 获取排行榜长度
func (rs *RankSkip) GetRankLen() uint64 {
	return rs.skipList.Len()
}

func (rs *RankSkip) UpsetRank(upsetRankData []*rpc.RankData) (addCount int32, modifyCount int32) {
	addList := make([]*RankData, 0, 1)
	updateList := make([]*RankData, 0, 1)
	for _, upsetData := range upsetRankData {
		changeData, changeType := rs.upsetRank(upsetData)
		if changeData == nil {
			continue
		}

		switch changeType {
		case RankDataAdd:
			addList = append(addList, changeData)
		case RankDataUpdate:
			updateList = append(updateList, changeData)
		}
	}

	if len(addList) > 0 {
		rs.rankModule.OnEnterRank(rs, addList)
	}

	if len(updateList) > 0 {
		rs.rankModule.OnChangeRankData(rs, updateList)
	}

	addCount = int32(len(addList))
	modifyCount = int32(len(updateList))

	rs.pickExpireKey()
	return
}

// UpsetRank 更新玩家排行数据,返回变化后的数据及变化类型
func (rs *RankSkip) upsetRank(upsetData *rpc.RankData) (*RankData, RankDataChangeType) {
	rankNode, ok := rs.mapRankData[upsetData.Key]
	if ok == true {
		//找到的情况对比排名数据是否有变化,无变化进行data更新,有变化则进行删除更新
		if compareIsEqual(rankNode.SortData, upsetData.SortData) {
			rankNode.Data = upsetData.GetData()
			return nil, RankDataNone
		}

		if upsetData.Data == nil {
			upsetData.Data = rankNode.Data
		}
		rs.skipList.Delete(rankNode)
		ReleaseRankData(rankNode)

		newRankData := NewRankData(rs.isDes, upsetData)
		rs.skipList.Insert(newRankData)
		rs.mapRankData[upsetData.Key] = newRankData

		//刷新有效期
		rs.rankDataExpire.PushOrRefreshExpireKey(upsetData.Key)

		return newRankData, RankDataUpdate
	}

	if rs.checkInsertAndReplace(upsetData) {
		newRankData := NewRankData(rs.isDes, upsetData)
		rs.skipList.Insert(newRankData)
		rs.mapRankData[upsetData.Key] = newRankData
		rs.rankDataExpire.PushOrRefreshExpireKey(upsetData.Key)
		return newRankData, RankDataAdd
	}

	return nil, RankDataNone
}

// DeleteRankData 删除排行数据
func (rs *RankSkip) DeleteRankData(delKeys []uint64) int32 {
	removeRankData := make([]*RankData, 0, 1)
	//预统计处理，进行回调
	for _, key := range delKeys {
		rankData, ok := rs.mapRankData[key]
		if ok == false {
			continue
		}

		removeRankData = append(removeRankData, rankData)
	}
	rs.rankModule.OnLeaveRank(rs, removeRankData)

	//从排行榜中删除
	for _, rankData := range removeRankData {
		rs.skipList.Delete(rankData)
		delete(rs.mapRankData, rankData.Key)
		rs.rankDataExpire.RemoveExpireKey(rankData.Key)
		ReleaseRankData(rankData)
	}

	rs.pickExpireKey()
	return int32(len(removeRankData))
}

// GetRankNodeData 获取,返回排名节点与名次
func (rs *RankSkip) GetRankNodeData(findKey uint64) (*RankData, uint64) {
	rankNode, ok := rs.mapRankData[findKey]
	if ok == false {
		return nil, 0
	}

	_, index := rs.skipList.GetWithPosition(rankNode)
	return rankNode, index
}

// GetRankNodeDataByPos 获取,返回排名节点与名次
func (rs *RankSkip) GetRankNodeDataByRank(rank uint64) (*RankData, uint64) {
	rankNode := rs.skipList.ByPosition(rank)
	if rankNode == nil {
		return nil, 0
	}

	return rankNode.(*RankData), rank
}

// GetRankKeyPrevToLimit 获取key前count名的数据
func (rs *RankSkip) GetRankKeyPrevToLimit(findKey, count uint64, result *rpc.RankDataList) error {
	if rs.GetRankLen() <= 0 {
		return fmt.Errorf("rank[", rs.rankId, "] no data")
	}

	findData, ok := rs.mapRankData[findKey]
	if ok == false {
		return fmt.Errorf("rank[", rs.rankId, "] no data")
	}

	_, rankPos := rs.skipList.GetWithPosition(findData)
	iter := rs.skipList.Iter(findData)
	iterCount := uint64(0)
	for iter.Prev() && iterCount < count {
		rankData := iter.Value().(*RankData)
		result.RankPosDataList = append(result.RankPosDataList, &rpc.RankPosData{
			Key:      rankData.Key,
			Rank:     rankPos - iterCount,
			SortData: rankData.SortData,
			Data:     rankData.Data,
		})
		iterCount++
	}

	return nil
}

// GetRankKeyPrevToLimit 获取key前count名的数据
func (rs *RankSkip) GetRankKeyNextToLimit(findKey, count uint64, result *rpc.RankDataList) error {
	if rs.GetRankLen() <= 0 {
		return fmt.Errorf("rank[", rs.rankId, "] no data")
	}

	findData, ok := rs.mapRankData[findKey]
	if ok == false {
		return fmt.Errorf("rank[", rs.rankId, "] no data")
	}

	_, rankPos := rs.skipList.GetWithPosition(findData)
	iter := rs.skipList.Iter(findData)
	iterCount := uint64(0)
	for iter.Next() && iterCount < count {
		rankData := iter.Value().(*RankData)
		result.RankPosDataList = append(result.RankPosDataList, &rpc.RankPosData{
			Key:      rankData.Key,
			Rank:     rankPos + iterCount,
			SortData: rankData.SortData,
			Data:     rankData.Data,
		})
		iterCount++
	}

	return nil
}

// GetRankList 获取排行榜数据,startPos开始的count个数据
func (rs *RankSkip) GetRankDataFromToLimit(startPos, count uint64, result *rpc.RankDataList) error {
	if rs.GetRankLen() <= 0 {
		return fmt.Errorf("rank[", rs.rankId, "] no data")
	}

	if result.RankDataCount < startPos {
		startPos = result.RankDataCount - 1
	}

	iter := rs.skipList.IterAtPosition(startPos)
	iterCount := uint64(0)
	for iter.Next() && iterCount < count {
		rankData := iter.Value().(*RankData)
		result.RankPosDataList = append(result.RankPosDataList, &rpc.RankPosData{
			Key:      rankData.Key,
			Rank:     iterCount + startPos,
			SortData: rankData.SortData,
			Data:     rankData.Data,
		})
		iterCount++
	}

	return nil
}

// checkCanInsert 检查是否能插入
func (rs *RankSkip) checkInsertAndReplace(upsetData *rpc.RankData) bool {
	//maxLen为0，不限制长度
	if rs.maxLen == 0 {
		return true
	}

	//没有放满,则进行插入
	rankLen := rs.skipList.Len()
	if rs.maxLen > rankLen {
		return true
	}

	//已经放满了,进行数据比较
	lastPosData := rs.skipList.ByPosition(rankLen - 1)
	lastRankData := lastPosData.(*RankData)
	moreThanFlag := compareMoreThan(upsetData.SortData, lastRankData.SortData)
	//降序排列,比最后一位小,不能插入                 升序排列,比最后一位大,不能插入
	if (rs.isDes == true && moreThanFlag < 0) || (rs.isDes == false && moreThanFlag > 0) || moreThanFlag == 0 {
		return false
	}

	//移除最后一位
	//回调模块，该RandData从排行中删除
	rs.rankDataExpire.RemoveExpireKey(lastRankData.Key)
	rs.rankModule.OnLeaveRank(rs, []*RankData{lastRankData})
	rs.skipList.Delete(lastPosData)
	delete(rs.mapRankData, lastRankData.Key)
	ReleaseRankData(lastRankData)
	return true
}
