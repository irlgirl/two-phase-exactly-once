package exactly_once

import (
)

type queryData struct {
  uuid       int
}

type Message interface {
  GenerateEvent() binlogEvent
}

type prepareMsg struct {
  msg queryData
}

func (m prepareMsg) GenerateEvent() binlogEvent {
  return doPrepareQueryEvent{msg: m.msg, slot_suggestion: slotMetaData{uuid: GenerateUUID()}}
}

type prepareMsgResponse struct {
  msg queryData
  slot slotMetaData
}

func (m prepareMsgResponse) GenerateEvent() binlogEvent {
  return sendQueryCommitEvent{msg: m.msg, slot: m.slot}
}

type commitMsg struct {
  msg queryData
  slot slotMetaData
}

func (m commitMsg) GenerateEvent() binlogEvent {
  return doCommitQueryEvent{msg: m.msg, slot: m.slot}
}

type commitMsgResponse struct {
  msg queryData
}

func (m commitMsgResponse) GenerateEvent() binlogEvent {
  return msgAckEvent{msg: m.msg}
}

