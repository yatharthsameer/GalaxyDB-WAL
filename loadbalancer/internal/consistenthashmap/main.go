package consistenthashmap

const (
	Slots = 512
	K     = 9
)

// New hash functions
func H(i uint32) uint32 {
	i = (((i >> 16) ^ i) * 0x45d9f3b) >> 16 ^ i
	return i
}

func assistH(i, j uint32) uint32 {
	return H(i + H(j))
}
func hashRequest(i int) int {
	return int(assistH(uint32(i), uint32(i))) 
}

func hashVirtualServer(i, j int) int {
	return int(assistH(uint32(i), uint32(j))) 
}
type ConsistentHashMap struct {
	virtualServers [Slots]int
}

func (hm *ConsistentHashMap) Init() {
	for i := 0; i < Slots; i++ {
		hm.virtualServers[i] = -1
	}
}

func (hm *ConsistentHashMap) findEmptyServerSlot(hashValue int) int {
	slot := hashValue % Slots
	i := 0
	for hm.virtualServers[slot] != -1 && i <= Slots {
		slot = (slot + 1) % Slots
		i++
	}
	if i > Slots {
		return -1
	}
	return slot
}

// Updated to use the Phi hash function


func (hm *ConsistentHashMap) AddServer(serverID int) {
	for j := 0; j < K; j++ {
		slot := hm.findEmptyServerSlot(hashVirtualServer(serverID, j))
		hm.virtualServers[slot] = serverID
	}
}

func (hm *ConsistentHashMap) GetServerForRequest(requestID int) int {
	slot := hashRequest(requestID) % Slots
	i := 0
	for hm.virtualServers[slot] == -1 && i <= Slots {
		slot = (slot + 1) % Slots
		i++
	}
	if i > Slots {
		return -1
	}
	return hm.virtualServers[slot]
}

func (hm *ConsistentHashMap) RemoveServer(serverID int) {
	for j := 0; j < K; j++ {
		virtualServerHash := hashVirtualServer(serverID, j)
		slot := virtualServerHash % Slots
		if hm.virtualServers[slot] == serverID {
			hm.virtualServers[slot] = -1

			nextSlot := (slot + 1) % Slots
			for hm.virtualServers[nextSlot] == serverID {
				hm.virtualServers[nextSlot] = -1
				nextSlot = (nextSlot + 1) % Slots
			}
		}
	}
}
