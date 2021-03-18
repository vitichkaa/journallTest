package journallv2

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/night-codes/types.v1"
	"reflect"
	"sync"
	"time"
)

type (
	obj map[string]interface{}
	arr []interface{}

	// Settings intended for data transmission into the Create method of package
	settings struct {
		Name           string
		DB             *mgo.Database
		Interval       uint64
		Indexes        []mgo.Index
		CollectionInfo *mgo.Collection
	}

	infoCollection struct {
		ID                     uint64   `bson:"_id"`                    //айди настроек
		CollectionsList        []string `bson:"collectionsList"`        //список коллекций
		ReverseCollectionsList []string `bson:"ReverseCollectionsList"` //список коллекций от конца до начала, для поиска
	}

	JournalCollection struct {
		settings settings
		info     infoCollection
		sync.Mutex
	}

	Query struct {
		queries []*mgo.Query
		skip    int
		limit   int
	}
)

//создание модуля
// ВНИМАНИЕ!!! ПРИ СОЗДАНИИ НОВОГО ЖУРНАЛА МОЖЕТ ПОНАДОБИТСЯ ДОПОЛНИТЕЛЬНАЯ ПЕРЕЗАГРУЗКА
func Create(name string, db *mgo.Database, interval uint64, indexes []mgo.Index, readOnly ...bool) *JournalCollection {
	//получение коллекции информации

	//создание модуля журнала
	journall := &JournalCollection{
		settings: settings{
			Name:           name,
			DB:             db,
			Interval:       interval,
			Indexes:        indexes,
			CollectionInfo: db.C(name + "-settings"),
		},
	}

	//получение информации с базы
	if err := journall.settings.CollectionInfo.FindId(1).One(&journall.info); err != nil {
		journall.info.ID = 1
	}

	if len(readOnly) > 0 && readOnly[0] {
		fmt.Println("journallv2.go Create() start readOnlymode")
		go func() {
			for range time.Tick(time.Minute * 1) {
				journall.updateCollectionsList()
			}
		}()
	}

	return journall
}

//insert в базу
func (jc *JournalCollection) Insert(id uint64, insert interface{}) error {
	collection := jc.settings.DB.C(jc.getCollectionName(id))

	for _, index := range jc.settings.Indexes {
		index.Background = true
		collection.EnsureIndex(index)
	}

	err := collection.Insert(insert)
	if err == nil {
		go jc.analizeCollectionList(id)
		return nil
	}

	return err
}

//поиск по ID
func (jc *JournalCollection) FindId(id uint64) *mgo.Query {
	return jc.settings.DB.C(jc.getCollectionName(id)).FindId(id)
}

//временно отключено
func (jc *JournalCollection) Update(selector, update interface{}) error {
	var errs []error
	jc.Lock()
	for k := range jc.info.ReverseCollectionsList {
		errs = append(errs, jc.settings.DB.C(jc.info.ReverseCollectionsList[k]).Update(selector, update))
	}
	jc.Unlock()

	//хотя бы один без ошибки
	oneWithNoError := false
	var lastError error
	for k := range errs {
		if errs[k] == nil {
			oneWithNoError = true
		} else {
			lastError = errs[k]
		}
	}

	if oneWithNoError {
		return nil
	}

	return lastError
}

//временно отключено
func (jc *JournalCollection) UpdateAll(selector, update interface{}) error {
	var errs []error
	jc.Lock()
	for k := range jc.info.ReverseCollectionsList {
		_, err := jc.settings.DB.C(jc.info.ReverseCollectionsList[k]).UpdateAll(selector, update)
		errs = append(errs, err)
	}
	jc.Unlock()

	//хотя бы один без ошибки
	oneWithNoError := false
	var lastError error
	for k := range errs {
		if errs[k] == nil {
			oneWithNoError = true
		} else {
			lastError = errs[k]
		}
	}

	if oneWithNoError {
		return nil
	}

	return lastError
}

//обновление по ID
func (jc *JournalCollection) UpdateId(id uint64, update interface{}) error {
	collection := jc.settings.DB.C(jc.getCollectionName(id))
	return collection.UpdateId(id, update)
}

func (jc *JournalCollection) UpsertId(id uint64, update interface{}) (err error) {
	collection := jc.settings.DB.C(jc.getCollectionName(id))
	_, err = collection.UpsertId(id, update)
	return err
}

func (jc *JournalCollection) Find(query interface{}) *Query {
	queries := []*mgo.Query{}
	jc.Lock()
	for k := range jc.info.ReverseCollectionsList {
		collection := jc.settings.DB.C(jc.info.ReverseCollectionsList[k])
		queries = append(queries, collection.Find(query))
	}
	jc.Unlock()
	return &Query{queries: queries}
}

func (q *Query) All(result interface{}) {
	varA := reflect.ValueOf(result)
	skip := q.skip
	limit := q.limit
	if limit == 0 {
		limit = 1000000000
	}

	if varA.Kind() != reflect.Ptr { // должен быть указатель
		fmt.Println("journallv2.go All() not KIND")
		return
	}
	varA = varA.Elem()                // берем значение по указателю
	if varA.Kind() != reflect.Slice { // и это значение должно быть slice
		fmt.Println("journallv2.go All() not SLICE")
		return
	}

	varB := reflect.MakeSlice(varA.Type(), 0, 0)
	for _, query := range q.queries {
		if skip == 0 {
			if err := query.Skip(skip).Limit(limit).All(result); err != nil {
				fmt.Println("journallv2.go All() error all:", err.Error())
			}
			varB = reflect.AppendSlice(varB, varA)
			limit -= varA.Len()
			if limit == 0 {
				break
			}
		} else {
			count, _ := query.Count()
			if count <= skip {
				skip -= count
			} else { //skip < count
				if err := query.Skip(skip).Limit(limit).All(result); err != nil {
					fmt.Println("journallv2.go All() error2 all:", err.Error())
				}
				varB = reflect.AppendSlice(varB, varA)
				limit -= varA.Len()
				if limit == 0 {
					break
				}
				skip = 0
			}
		}
	}
	varA.Set(varB)
}

func (q *Query) One(result interface{}) (err error) {
	for _, query := range q.queries {
		if n, error := query.Count(); error == nil && n > 0 {
			err = query.One(result)
			return
		}
	}
	return mgo.ErrNotFound
}

func (q *Query) Count() (n int, err error) {
	for _, query := range q.queries {
		if nn, error := query.Count(); error == nil {
			n += nn
		} else {
			err = error
			return
		}
	}
	return
}

func (q *Query) Distinct(key string, result interface{}) {
	varA := reflect.ValueOf(result)
	if varA.Kind() != reflect.Ptr { // должен быть указатель
		return
	}
	varA = varA.Elem()                // берем значение по указателю
	if varA.Kind() != reflect.Slice { // и это значение должно быть slice
		return
	}

	varB := reflect.MakeSlice(varA.Type(), 0, 0)
	for _, query := range q.queries {
		query.Distinct(key, result)
		fmt.Println(key, result)
		varB = reflect.AppendSlice(varB, varA)
	}
	varA.Set(varB)
}

func (q *Query) Select(selector interface{}) *Query {
	for k, query := range q.queries {
		q.queries[k] = query.Select(selector)
	}
	return q
}

func (q *Query) Sort(sort string) *Query {
	for k, query := range q.queries {
		q.queries[k] = query.Sort(sort)
	}
	return q
}

func (q *Query) Skip(n int) *Query {
	q.skip = n
	return q
}

func (q *Query) Limit(n int) *Query {
	q.limit = n
	return q
}

//получение имени коллекции по ID
func (jc *JournalCollection) getCollectionName(id uint64) string {
	numberCollection := uint64(id/jc.settings.Interval + 1)
	return jc.settings.Name + "-" + types.String(numberCollection)
}

//проверка на появление новой коллекции
func (jc *JournalCollection) analizeCollectionList(id uint64) {
	numberCollection := int(id/jc.settings.Interval + 1)

	//если появился новый номер коллекции
	jc.Lock()
	if numberCollection >= len(jc.info.CollectionsList)+1 {
		//еслть ли эта коллекция в информации
		collectioName := jc.settings.Name + "-" + types.String(numberCollection)
		exist := false

		for k := range jc.info.CollectionsList {
			if collectioName == jc.info.CollectionsList[k] {
				exist = true
			}
		}

		//если в наличии нет - добавляем и сохраняем в базу
		if !exist {
			jc.info.CollectionsList = append(jc.info.CollectionsList, collectioName)

			var reverseCollectionList []string
			lenCollectionList := len(jc.info.CollectionsList)

			if lenCollectionList > 0 {
				for i := 0; i < lenCollectionList; i++ {
					reverseCollectionList = append(reverseCollectionList, jc.info.CollectionsList[(lenCollectionList-1)-i])
				}

				jc.info.ReverseCollectionsList = reverseCollectionList
			}

			jc.settings.CollectionInfo.UpsertId(1, jc.info)
		}
	}
	jc.Unlock()
}

func (jc *JournalCollection) updateCollectionsList() {
	jc.Lock()
	if err := jc.settings.CollectionInfo.FindId(1).One(&jc.info); err != nil {
		jc.info.ID = 1
	}
	jc.Unlock()
}

func (q *Query) All2(result interface{}) {
	varA := reflect.ValueOf(result)
	slicev := varA.Elem()
	skip := q.skip
	limit := q.limit
	if limit == 0 {
		limit = 1000000000
	}

	if varA.Kind() != reflect.Ptr { // должен быть указатель
		fmt.Println("journallv2.go All() not KIND")
		return
	}
	varA = varA.Elem()                // берем значение по указателю
	if varA.Kind() != reflect.Slice { // и это значение должно быть slice
		fmt.Println("journallv2.go All() not SLICE")
		return
	}

	for _, query := range q.queries {
		varB :=  reflect.New(reflect.TypeOf(result).Elem())
		if skip == 0 {
			if err := query.Skip(skip).Limit(limit).All(varB.Interface()); err != nil {
				fmt.Println("journallv2.go All() error all:", err.Error())
			}
			slicev = reflect.AppendSlice(slicev, varB.Elem())

			limit -= varA.Len()
			if limit == 0 {
				break
			}
		} else {
			count, _ := query.Count()
			if count <= skip {
				skip -= count
			} else { //skip < count
				if err := query.Skip(skip).Limit(limit).All(result); err != nil {
					fmt.Println("journallv2.go All() error2 all:", err.Error())
				}
				varB = reflect.AppendSlice(varB, varA)
				limit -= varA.Len()
				if limit == 0 {
					break
				}
				skip = 0
			}
		}
	}
	varA.Set(slicev)
}