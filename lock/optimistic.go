package lock

import (
	"errors"
	"log"
	"os"
	"sync"
	"time"

	jefdb "github.com/jefreywo/golibs/db"
	"gorm.io/gorm"
)

func OptimisticLock() {
	db, err := jefdb.NewMysqlDB(&jefdb.MysqlDBConfig{
		User:         "root",
		Password:     "12345",
		Host:         "127.0.0.1",
		Port:         3306,
		Dbname:       "test",
		MaxIdleConns: 5,
		MaxOpenConns: 80,

		LogWriter:     os.Stdout,
		Colorful:      true,
		SlowThreshold: time.Second * 2,
		LogLevel:      "info",
	})
	if err != nil {
		log.Fatalln(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		err := updateUserBalance(db, 56)
		if err != nil {
			log.Println("updateUserBalance(100):", err)
		}
	}()

	go func() {
		defer wg.Done()
		err := updateUserBalance(db, 123)
		if err != nil {
			log.Println("updateUserBalance(200):", err)
		}
	}()
	wg.Wait()
}

var NoRowsAffectedError = errors.New("乐观锁更新数据失败")

func updateUserBalance(db *gorm.DB, reward int64) error {
	// select时要把当前版本号取出
	var u jefdb.JUser
	if err := db.Select("id,balance,version").First(&u, 1).Error; err != nil {
		return err
	}

	// 乐观锁更新失败时要重试，次数按实际需求设定
	var retry = 3
	var err error
	for i := 0; i < retry; i++ {
		err = db.Transaction(func(tx *gorm.DB) error {
			// 其他事务操作

			// 版本号更新
			result := tx.Table("j_user").
				Where("id = ? AND version = ?", u.Id, u.Version). // 判断版本号是否被更改
				Updates(map[string]interface{}{
					"balance": u.Balance + reward,
					"version": u.Version + 1, // 版本号要+1
				})

			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected == 0 {
				log.Println("更新失败, reward:", reward)
				return NoRowsAffectedError
			}

			return nil
		})

		if err == nil {
			break
		} else {
			if err == NoRowsAffectedError {
				time.Sleep(time.Second)
				continue
			}
			break
		}
	}

	return err
}
