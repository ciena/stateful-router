package router

// returns a list of nodes from 0 to highestKnownOrdinal, randomly but deterministically sorted based on the id
// the list is stable even with changes to highestKnownOrdinal
// (in other words, changing highestKnownOrdinal will add/remove entries, but will never reorder them)
func GetLocationArray(id uint64, highestKnownOrdinal uint32) []uint32 {
	array := []uint32{0}
	for i := uint32(1); i <= highestKnownOrdinal; i++ {
		pos := id % uint64(i+1)
		id /= uint64(i + 1)
		array = append(array[:pos], append([]uint32{i}, array[pos:]...)...)
	}
	return array
}

func BestNode(id uint64, ordinal uint32, nodes map[uint32]struct{}) uint32 {
	highest := ordinal
	for node := range nodes {
		if node > highest {
			highest = node
		}
	}
	array := GetLocationArray(id, highest)
	for {
		if _, have := nodes[array[0]]; have || array[0] == ordinal {
			return array[0]
		}
		array = array[1:]
	}
}

func BestOf(id uint64, nodes map[uint32]struct{}) uint32 {
	var highest uint32
	for node := range nodes {
		if node > highest {
			highest = node
		}
	}
	array := GetLocationArray(id, highest)
	for {
		if _, have := nodes[array[0]]; have {
			return array[0]
		}
		array = array[1:]
	}
}
