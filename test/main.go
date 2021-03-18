package main

import (
	"fmt"
	mongo "github.com/night-codes/mgo-wrapper"
	"gopkg.in/mgo.v2"
	"journallv2"
)

type (
	obj map[string]interface{}
)

var db = mongo.DB("testJournall")
var collection = journallv2.Create("players", db, 10, []mgo.Index{})

func main() {
	// isertTestData() добавляет тестовые данные

	Find() // Текущий вариант

	Find2() // Вариант уже без ошибки (сделал на скорую руку)
}

func Find() {
	var data []obj
	collection.Find(obj{"_id": obj{"$in": []uint64{7, 8, 9, 10, 11, 12}}}).All(&data)
	fmt.Println("Текущий:", data)
}

func Find2() {
	var data []obj
	collection.Find(obj{"_id": obj{"$in": []uint64{7, 8, 9, 10, 11, 12}}}).All2(&data)
	fmt.Println("Исправленый:", data)
}

func isertTestData() {
	for i := 0; i < 20; i++ {
		if err := collection.Insert(uint64(i), obj{"_id": i, "login": i}); err != nil {
			fmt.Println("insert err: ", err)
		}
	}
}
