package exactly_once

import (
	"pgregory.net/rapid"
)

type binlogEvent interface {
  Apply(*Server) bool
  ApplyAfterCommit(*Server, *rapid.T)
}

type sendQueryCommitEvent struct {
  msg  queryData
  slot slotMetaData
}

type doPrepareQueryEvent struct {
  msg             queryData
  slot_suggestion slotMetaData
}

type msgAckEvent struct {
  msg queryData
}

type doCommitQueryEvent struct {
  msg  queryData
  slot slotMetaData
}

type createQueryEvent struct {
  msg queryData
}

func (m doPrepareQueryEvent) Apply(s *Server) bool {
  s.slots.GetOrAlloc(m.msg, m.slot_suggestion)
  return true;
}

func (m doPrepareQueryEvent) ApplyAfterCommit(s *Server, t *rapid.T) {
  slot := s.slots.GetOrAlloc(m.msg, m.slot_suggestion)

  msg := prepareMsgResponse{msg: m.msg, slot: slot}
  s.messages_net.PushBack(msg)
}

func (e sendQueryCommitEvent) Apply(s *Server) bool {
  if s.messages_prepare.Len() == 0 || s.messages_prepare.Front().msg != e.msg {
    return false;
  }
  s.messages_prepare.PopFront()
  return true;
}

func (e sendQueryCommitEvent) ApplyAfterCommit(s *Server, t *rapid.T) {
  s.messages_commit.PushBack(commitMsg{msg: e.msg, slot: e.slot})
}

func (e createQueryEvent) Apply(s *Server) bool {
  s.messages_prepare.PushBack(prepareMsg{msg: e.msg})
  s.persistent_queries.PushBack(e.msg)
  return true;
}

func (e createQueryEvent) ApplyAfterCommit(s *Server, t *rapid.T) {
}

func (e doCommitQueryEvent) Apply(s *Server) bool {
  return true;
}

func (e doCommitQueryEvent) ApplyAfterCommit(s *Server, t *rapid.T) {
  s.messages_net.PushBack(commitMsgResponse{msg: e.msg})
  if !s.slots.Validate(e.msg, e.slot) {
    // assume applied
    t.Logf("message slot is invalid. message: %v, slot %v", e.msg, e.slot)
    return
  }
  t.Logf("apply payload message %v", e.msg.uuid)
  s.slots.Destroy(e.msg)
  s.applied_cmds.PushBack(e.msg)
}

func (e msgAckEvent) Apply(s *Server) bool {
  if s.messages_commit.Len() == 0 || s.messages_commit.Front().msg != e.msg {
    return false
  }
  s.messages_commit.PopFront()
  return true;
}

func (e msgAckEvent) ApplyAfterCommit(s *Server, t *rapid.T) {
  s.finished_persistent_queries++;
}
