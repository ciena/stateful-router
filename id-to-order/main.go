package main

import "fmt"

var divisors = [...]uint8{1, 1, 2, 3, 2, 5, 1, 7, 2, 3, 1, 11, 1, 13, 1, 1, 2, 17, 1, 19, 1, 1, 1, 23, 1, 5, 1, 3, 1, 29, 1, 31, 2, 1, 1, 1, 1, 37, 1, 1, 1, 41, 1, 43, 1, 1, 1}

var primes = [...]uint8{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43}
var isPrime = map[uint8]uint8{
	2:  0,
	3:  1,
	5:  2,
	7:  3,
	11: 4,
	13: 5,
	17: 6,
	19: 7,
	23: 8,
	29: 9,
	31: 10,
	37: 11,
	41: 12,
	43: 13,
}

const maxRandom = 1 * 2 * 3 * 1 * 5 * 1 * 7 * 2 * 3 * 1 * 11 * 1 * 13 * 1 * 1 * 2 * 17 * 1 * 19 * 1 * 1 * 1 * 23 * 1 * 5 * 1 * 3 * 1 * 29 * 1 * 31 * 2 * 1 * 1 * 1 * 1 * 37 * 1 * 1 * 1 * 41 * 1 * 43 * 1 * 1 * 1

func main() {
	for i := 0; i < 1; i++ {
		fmt.Println(i, generateArray(uint64(i), 1))
	}
	fmt.Println()

	for i := 0; i < 2; i++ {
		fmt.Println(i, generateArray(uint64(i), 2))
	}
	fmt.Println()

	for i := 0; i < 2*3; i++ {
		fmt.Println(i, generateArray(uint64(i), 3))
	}
	fmt.Println()

	for i := 0; i < 1*2*3*2; i++ {
		fmt.Println(i, generateArray(uint64(i), 4))
	}
	fmt.Println()

	for i := 0; i < 1*2*3*2*5; i++ {
		fmt.Println(i, generateArray(uint64(i), 5))
	}
	fmt.Println()

	for i := 0; i < 1*2*3*2*5*1; i++ {
		fmt.Println(i, generateArray(uint64(i), 6))
	}
	fmt.Println()

	sanityTest(6)
}

func generateArray(randomness uint64, width uint8) []uint8 {
	if width > 46 {
		panic("cannot have more than 46 nodes")
	}

	//randomNumber := 0
	//theoreticalSetSize := 1

	array1 := []uint8{0}
	for i := uint8(1); i < width; i++ {
		random := randomness
		if i == 2 { // 3
			random = randomness
		}
		if i == 3 { // 4
			if randomness%12 < 4 {
				random = randomness
			} else {
				random = 7 - (randomness%12-4)/2
			}
		}
		if i == 4 { // 5
			random = randomness
			//fmt.Println(random)
		}
		if i == 5 { // 6
			random = randomness / 10
			//fmt.Println(random)
			//fmt.Println(randomness, random, random%uint64(i+1))
		}
		array1 = insert(array1, i, i-uint8(random%uint64(i+1)))
	}
	return array1

	array := []uint8{0}
	usedRandomness := make(map[uint8][]uint8)
	previousRandom := uint8(0)
	for currentNodeID := uint8(1); currentNodeID < width; currentNodeID++ {
		possiblePositions := currentNodeID + 1
		result, multi, otherResult, otherMulti := generateRandom(usedRandomness, &randomness, possiblePositions)
		result, multi, otherResult, otherMulti, previousRandom = result, multi, otherResult, otherMulti, previousRandom

		//fmt.Println(result, multi, otherResult, otherMulti)
		//fmt.Println(result, multi, otherResult, otherMulti)
		//fmt.Println(previousRandom, otherResult, otherMulti)
		position := uint8((result) % uint64(possiblePositions))

		array = insert(array, currentNodeID, position)
		//fmt.Println(array)
		previousRandom = position
	}
	//fmt.Println(usedRandomness)

	return array
}

func generateRandom(randomness map[uint8][]uint8, randomSource *uint64, number uint8) (uint64, uint64, uint64, uint64) {
	currentNum := number

	result := uint64(0)
	resultMultiplier := uint64(1)

	remainingResult := uint64(0)
	remainingResultMultiplier := uint64(1)

	// if not prime
	primeID := uint8(0)
	for prime := primes[primeID]; prime <= currentNum; prime = primes[primeID] {
		slice := randomness[primeID]

		i := 0
		// re-use randomness where possible, using prime divisors
		for ; currentNum != 0 && currentNum%prime == 0; currentNum /= prime {
			var random uint8
			if i >= len(slice) {
				random = getRandom(randomSource, prime)
				slice = append(slice, random)
				randomness[primeID] = slice
			} else {
				random = slice[i]
			}

			//fmt.Println("include:", random, prime)
			result += uint64(random) * resultMultiplier
			resultMultiplier *= uint64(prime)

			i++
		}
		//use up remaining randomness
		for ; i < len(slice); i++ {
			random := slice[i]

			//fmt.Println("exclude:", random, prime)
			remainingResult += uint64(random) * remainingResultMultiplier
			remainingResultMultiplier *= uint64(prime)
		}

		//go to next prime
		primeID++
	}

	for ; ; primeID++ {
		slice, have := randomness[primeID]
		if !have {
			break
		}
		prime := primes[primeID]
		for _, random := range slice {
			//fmt.Println("exclude:", random, prime)
			remainingResult += uint64(random) * remainingResultMultiplier
			remainingResultMultiplier *= uint64(prime)
		}
	}

	return result, resultMultiplier, remainingResult, remainingResultMultiplier
}

func getRandomDivisor(randomness map[uint8][]uint8, randomSource *uint64, divisor, position uint8) uint8 {
	slice := randomness[divisor]
	if position < uint8(len(slice)) {
		return slice[position]
	}
	ret := getRandom(randomSource, divisor)
	randomness[divisor] = append(slice, ret)
	return ret
}

func insert(array []uint8, val, pos uint8) []uint8 {
	return append(array[:pos], append([]uint8{val}, array[pos:]...)...)
}

func getRandom(randomness *uint64, max uint8) uint8 {
	ret := *randomness % uint64(max)
	*randomness /= uint64(max)
	return uint8(ret)
}

func sanityTest(width uint8) {
	size := uint64(1)
	for i := uint8(1); i <= width; i++ {
		size *= uint64(divisors[i])
	}

	failed := false
	for numOffline := uint8(0); numOffline < width; numOffline++ {
		fmt.Println("with", numOffline, "offline")
		for i := uint8(0); i < width; i++ {
			offline := make(map[uint8]bool)
			for j := uint8(0); j < numOffline; j++ {
				offline[(j+i)%width] = true
			}
			fmt.Println("with", offline, "offline")
			fmt.Println("start")
			counts := make(map[uint8]uint64)
			for i := uint64(0); i < size; i++ {
				array := generateArray(i, width)
				for offline[array[0]] {
					array = array[1:]
				}
				node := array[0]
				counts[node] = counts[node] + 1
			}

			expected := size / uint64(width-numOffline)
			for node, count := range counts {
				if count != expected {
					fmt.Printf("For node %d: Expected %d, but was %d!!!!!!!\n", node, expected, count)
					failed = true
				}
				fmt.Println(count, expected)
			}

			fmt.Println("end")
			if numOffline == 0 {
				break
			}
		}
	}
	fmt.Println("--- end sanity test ---")
	if failed {
		fmt.Println("FAILED")
	} else {
		fmt.Println("PASSED")
	}
}
