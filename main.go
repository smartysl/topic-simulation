package main

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
	"math"
)

type Post struct {
	TopicID    int
	UserID     int
	Lat        float64
	Lng        float64
	LikeNum    int
	ScoreNum   int
	CreateTime time.Time
	UpdateTime time.Time
}

type User struct {
	UserID int
	Lat    float64
	Lng    float64
}

type Pair struct {
    Key   int
    Value float64
}

type PairList []Pair

func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }

var mu sync.Mutex

func generateRandomPosts(userID int, numPosts int) []Post {
	posts := make([]Post, numPosts)
	for i := 0; i < numPosts; i++ {
		posts[i] = Post{
			TopicID:    rand.Intn(1000000),
			UserID:     userID,
			Lat:        rand.Float64()*180 - 90,
			Lng:        rand.Float64()*360 - 180,
			LikeNum:    rand.Intn(1000),
			ScoreNum:   rand.Intn(1000),
			CreateTime: time.Now(),
			UpdateTime: time.Now(),
		}
	}
	return posts
}

func simulateUserPosts(userID int, wg *sync.WaitGroup, postsChan chan<- []Post) {
	defer wg.Done()
	for {
		numPosts := rand.Intn(51) + 50
		posts := generateRandomPosts(userID, numPosts)
		postsChan <- posts
		time.Sleep(1 * time.Minute)
	}
}

func findNearbyUsersPosts(userID int, users []User, posts []Post) []Post {
	nearbyUsers := findNearbyUsers(userID, users, 5)
	var nearbyPosts []Post
	for _, post := range posts {
		for _, user := range nearbyUsers {
			if post.UserID == user.UserID {
				nearbyPosts = append(nearbyPosts, post)
			}
		}
	}

	sort.Slice(nearbyPosts, func(i, j int) bool {
		return nearbyPosts[i].LikeNum > nearbyPosts[j].LikeNum
	})
	return nearbyPosts
}

func findNearbyUsers(userID int, users []User, num int) []User {
	calculateDistance := func(user1 User, user2 User) float64 {
		radLat1 := math.Pi * user1.Lat / 180
		radLong1 := math.Pi * user1.Lng / 180
		radLat2 := math.Pi * user2.Lat / 180
		radLong2 := math.Pi * user2.Lng / 180

		dLat := radLat2 - radLat1
		dLon := radLong2 - radLong1

		a := (math.Sin(dLat/2) * math.Sin(dLat/2)) + (math.Cos(radLat1) * math.Cos(radLat2) * math.Sin(dLon/2) * math.Sin(dLon/2))
		c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

		r := float64(6371) // Earth's radius

		return r * c
	}

	targetUser := users[userID]
	distances := PairList{}

	for _, user := range users {
		if user.UserID != userID {
			distance := calculateDistance(targetUser, user)
			distances = append(distances, Pair{Key: user.UserID, Value: distance})
		}
	}

	sort.Sort(distances)
	nearbyUserIDs := []int{}
	for _, pair := range distances[:num] {
		nearbyUserIDs = append(nearbyUserIDs, pair.Key);
	}

	nearbyUsers := []User{}
	for _, user := range users {
		for _, uid := range nearbyUserIDs {
			if user.UserID == uid {
				nearbyUsers = append(nearbyUsers, user)
				break
			}
		}
	}

	return nearbyUsers
}

func main() {
	userNum := 100000
	var wg sync.WaitGroup
	postsChan := make(chan []Post, 100000)
	users := make([]User, userNum)

	for i := 0; i < 10; i++ {
		users[i] = User{
			UserID: i,
			Lat:    rand.Float64()*180 - 90,
			Lng:    rand.Float64()*360 - 180,
		}
	}

	for i := 0; i < userNum; i++ {
		wg.Add(1)
		go simulateUserPosts(i, &wg, postsChan)
	}

	var allPosts []Post
	go func() {
		defer mu.Unlock()
		for posts := range postsChan {
			mu.Lock()
			allPosts = append(allPosts, posts...)
			mu.Unlock()
		}
	}()

	time.Sleep(1 * time.Second)

	mu.Lock()
	userID := 1
	nearbyPosts := findNearbyUsersPosts(userID, users, allPosts)
	fmt.Printf("Nearby posts for user %d: %+v\n", userID, nearbyPosts)
	mu.Unlock()

	wg.Wait()
}
