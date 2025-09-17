package oplog

import (
	"encoding/json"
	"fmt"
	"time"
)

// HybridLogicalClock (HLC) реализация
type HybridLogicalClock struct {
	physicalTime int64  // Unix nanoseconds
	logicalTime  int64  // Логический счетчик
	nodeID       string // Идентификатор узла
}

// NewHLClock создает новый HLC
func NewHLClock(nodeID string) *HybridLogicalClock {
	return &HybridLogicalClock{
		physicalTime: time.Now().UnixNano(),
		logicalTime:  0,
		nodeID:       nodeID,
	}
}

// NewHLClockFromParams создает HLC из параметров
func NewHLClockFromParams(physicalTime, logicalTime int64, nodeID string) *HybridLogicalClock {
	return &HybridLogicalClock{
		physicalTime: physicalTime,
		logicalTime:  logicalTime,
		nodeID:       nodeID,
	}
}

// GetPhysicalTime возвращает физическое время
func (hlc *HybridLogicalClock) GetPhysicalTime() int64 {
	return hlc.physicalTime
}

// Tick обновляет часы для нового события
func (hlc *HybridLogicalClock) Tick() *HybridLogicalClock {
	now := time.Now().UnixNano()

	if now > hlc.physicalTime {
		hlc.physicalTime = now
		hlc.logicalTime = 0
	} else {
		hlc.logicalTime++
	}

	return &HybridLogicalClock{
		physicalTime: hlc.physicalTime,
		logicalTime:  hlc.logicalTime,
		nodeID:       hlc.nodeID,
	}
}

// Update обновляет часы при получении события от другого узла
func (hlc *HybridLogicalClock) Update(remote *HybridLogicalClock) *HybridLogicalClock {
	now := time.Now().UnixNano()

	maxPhysical := max(hlc.physicalTime, remote.physicalTime, now)

	var newLogical int64
	if maxPhysical == hlc.physicalTime && maxPhysical == remote.physicalTime {
		newLogical = max(hlc.logicalTime, remote.logicalTime) + 1
	} else if maxPhysical == hlc.physicalTime {
		newLogical = hlc.logicalTime + 1
	} else if maxPhysical == remote.physicalTime {
		newLogical = remote.logicalTime + 1
	} else {
		newLogical = 0
	}

	hlc.physicalTime = maxPhysical
	hlc.logicalTime = newLogical

	return &HybridLogicalClock{
		physicalTime: maxPhysical,
		logicalTime:  newLogical,
		nodeID:       hlc.nodeID,
	}
}

// Compare сравнивает два HLC (returns -1, 0, 1)
func (hlc *HybridLogicalClock) Compare(other *HybridLogicalClock) int {
	if hlc.physicalTime < other.physicalTime {
		return -1
	}
	if hlc.physicalTime > other.physicalTime {
		return 1
	}

	if hlc.logicalTime < other.logicalTime {
		return -1
	}
	if hlc.logicalTime > other.logicalTime {
		return 1
	}

	// При равных временах сравниваем nodeID для детерминированности
	if hlc.nodeID < other.nodeID {
		return -1
	}
	if hlc.nodeID > other.nodeID {
		return 1
	}

	return 0
}

// String представление HLC
func (hlc *HybridLogicalClock) String() string {
	return fmt.Sprintf("HLC{pt:%d,lt:%d,node:%s}", hlc.physicalTime, hlc.logicalTime, hlc.nodeID)
}

// ToBytes сериализует HLC в bytes для хранения
func (hlc *HybridLogicalClock) ToBytes() ([]byte, error) {
	return json.Marshal(hlc)
}

// FromBytes десериализует HLC из bytes
func (hlc *HybridLogicalClock) FromBytes(data []byte) error {
	return json.Unmarshal(data, hlc)
}
