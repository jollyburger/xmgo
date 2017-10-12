package xmgo

import (
	"errors"
	"fmt"
	"sync"
	"time"

	mgo "gopkg.in/mgo.v2"
)

type MgoPool struct {
	sync.RWMutex
	sessions chan *mgo.Session
	poolSize int
	factory  Factory
}

type Factory func() (*mgo.Session, error)

func adaptor(action string, addr []string, timeout time.Duration, username string, passwd string) Factory {
	ff := func() (*mgo.Session, error) {
		var (
			session *mgo.Session
			err     error
		)
		if len(addr) == 0 {
			return nil, errors.New("mongodb address is empty")
		}
		switch action {
		case "single":
			if timeout == 0 {
				session, err = mgo.Dial(addr[0])
			} else {
				session, err = mgo.DialWithTimeout(addr[0], timeout)
			}
		case "cluster":
			dialinfo := mgo.DialInfo{
				Addrs:    addr,
				Timeout:  timeout,
				Username: username,
				Password: passwd,
			}
			session, err = mgo.DialWithInfo(&dialinfo)
		}
		return session, err
	}
	return ff
}

func InitMgoPool(mode string, addr []string, timeout time.Duration, username string, passwd string, pool_size int) *MgoPool {
	mgo_pool := new(MgoPool)
	mgo_pool.poolSize = pool_size
	mgo_pool.sessions = make(chan *mgo.Session, pool_size)
	mgo_pool.factory = adaptor(mode, addr, timeout, username, passwd)
	return mgo_pool
}

func (mp *MgoPool) getSessions() chan *mgo.Session {
	mp.RLock()
	defer mp.RUnlock()
	return mp.sessions
}

func (mp *MgoPool) Get() (*mgo.Session, error) {
	sess := mp.getSessions()
	if sess == nil {
		return nil, errors.New("mongo session pool is nil")
	}
	select {
	case session := <-sess:
		if session == nil {
			return nil, errors.New("get nil session")
		}
		return session, nil
	default:
		session, err := mp.factory()
		return session, err
	}
}

func (mp *MgoPool) Put(session *mgo.Session) {
	sess := mp.getSessions()
	if sess == nil {
		sess = make(chan *mgo.Session, mp.poolSize)
		mp.sessions = sess
	}
	mp.Lock()
	defer mp.Unlock()
	if len(sess) >= mp.poolSize {
		session.Close()
	} else {
		sess <- session
	}
	return
}

func (mp *MgoPool) Dump() {
	fmt.Printf("mgo pool length: %d", mp.poolSize)
}

func (mp *MgoPool) CloseSession(session *mgo.Session) {
	session.Close()
}

func (mp *MgoPool) CloseAll() {
	mp.Lock()
	defer mp.Unlock()
	for sess := range mp.sessions {
		sess.Close()
	}
	close(mp.sessions)
}

func (mp *MgoPool) Len() int {
	mp.RLock()
	defer mp.RUnlock()
	return len(mp.sessions)
}
