package exactly_once

import (
	"github.com/gammazero/deque"

	"pgregory.net/rapid"
)

type slotMetaData struct {
  target_id int
  uuid int
}

type Binlog struct {
  uncommited   *deque.Deque[binlogEvent]
  commited     *deque.Deque[binlogEvent]
}

func (b *Binlog) Push(e binlogEvent) {
  b.uncommited.PushBack(e)
}

type ExactlyOnceSlots struct {
  slots map[queryData]slotMetaData
}

func (s *ExactlyOnceSlots) GetOrAlloc(m queryData, sugg slotMetaData) slotMetaData {
  slot, ok := s.slots[m]
  if !ok {
    s.slots[m] = sugg
    slot = sugg
  }
  return slot
}


func (s *ExactlyOnceSlots) Destroy(m queryData) {
  delete(s.slots, m)
}

func (s *ExactlyOnceSlots) Validate(m queryData, meta slotMetaData) bool {
  return s.slots[m] == meta
}

type Server struct {
  // "State"
  slots              ExactlyOnceSlots
  messages_prepare   *deque.Deque[prepareMsg]
  messages_commit    *deque.Deque[commitMsg]
  applied_cmds       *deque.Deque[queryData]
  persistent_queries *deque.Deque[queryData]
  finished_persistent_queries int


  // "External"
  binlog        Binlog
  messages_net  *deque.Deque[Message]
}

func NewServer() Server {
  return Server {
    binlog: Binlog {
      uncommited: deque.New[binlogEvent](),
      commited: deque.New[binlogEvent](),
    },
    messages_net: deque.New[Message](),
    slots: ExactlyOnceSlots {
      slots: make(map[queryData]slotMetaData),
    },
    messages_prepare: deque.New[prepareMsg](),
    messages_commit: deque.New[commitMsg](),
    applied_cmds: deque.New[queryData](),
    persistent_queries: deque.New[queryData](),
    finished_persistent_queries : 0,
  }
}

func (s *Server) HandleMsg(m Message, t *rapid.T) {
  event := m.GenerateEvent()
  status := event.Apply(s)
  if (status) {
    s.binlog.Push(event)
  }
}

func (s *Server) RetryQueries() {
  if s.messages_prepare.Len() > 0 {
    s.messages_net.PushBack(s.messages_prepare.Front())
  }
  if s.messages_commit.Len() > 0 {
    s.messages_net.PushBack(s.messages_commit.Front())
  }
}

func (s *Server) ProducePersistentQuery(t *rapid.T) {
  s.binlog.Push(createQueryEvent{msg: queryData{uuid: GenerateUUID()}})
}

func (s *Server) CommitRandom(t *rapid.T) {
  commit := rapid.IntRange(0, s.binlog.uncommited.Len()).Draw(t, "events to commit")
  for i := 0; i < commit; i++ {
    event := s.binlog.uncommited.PopFront()

    event.ApplyAfterCommit(s, t)
    s.binlog.commited.PushBack(event)
  }
}
