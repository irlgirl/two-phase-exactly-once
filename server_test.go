package exactly_once

import (
	"testing"
	"pgregory.net/rapid"
)

type serverClientMachine struct {
  server Server
  client Server

  in_sync bool
}

func (m *serverClientMachine) Init(t *rapid.T) {
  m.server = NewServer()
  m.client = NewServer()
  m.in_sync = false
}

func (m *serverClientMachine) Check(t *rapid.T) {
  sync_completed := m.client.messages_commit.Len() == 0 && m.client.messages_prepare.Len() == 0

  last_uuid := -1
  for i := 0; i < m.server.applied_cmds.Len(); i++ {
    cmd := m.server.applied_cmds.At(i)

    if (cmd.uuid == last_uuid) {
      t.Fatalf("query %v executed twice", cmd.uuid)
    }
    if (cmd.uuid < last_uuid) {
      t.Fatalf("query with uuid %v executed before %v", last_uuid, cmd.uuid)
    }
    last_uuid = cmd.uuid
  }

  t.Logf("total applied events %v", m.server.applied_cmds.Len())
  if !sync_completed {
    return
  }

  if m.client.persistent_queries.Len() != m.server.applied_cmds.Len() {
    t.Fatalf("len not matched: client send %v queries, server applied %v queries",
              m.client.persistent_queries.Len(), m.server.applied_cmds.Len())
  }
  for i := 0; i < m.client.persistent_queries.Len(); i++ {
    sended_query := m.client.persistent_queries.At(i)
    applied_query := m.server.applied_cmds.At(i)
    if sended_query != applied_query {
      t.Fatalf("persisent queries at position %v mismatch: sended %v, applied %v", i, sended_query, applied_query)
    }
  }

  m.in_sync = false
}

func Truncated(server *Server, t *rapid.T) Server{
  if server.binlog.uncommited.Len() == 0 {
    t.Skip("no binlog to truncate")
    return *server
  }

  new_server := NewServer()
  new_server.binlog.commited = server.binlog.commited
  for i := 0; i < new_server.binlog.commited.Len(); i++ {
    event := server.binlog.commited.At(i)
    event.Apply(&new_server)
    event.ApplyAfterCommit(&new_server, t)
  }

  new_server.messages_net = server.messages_net
  return new_server
}

func (m *serverClientMachine) TruncateServer(t *rapid.T) {
  m.server = Truncated(&m.server, t)
}

func (m *serverClientMachine) TruncateClient(t *rapid.T) {
  m.client = Truncated(&m.client, t)
}

func (m *serverClientMachine) CommitServerBinlog(t *rapid.T) {
  m.server.CommitRandom(t)
}

func (m *serverClientMachine) CommitClientBinlog(t *rapid.T) {
  m.client.CommitRandom(t)
}

func ApplyMessage(from *Server, to *Server, t *rapid.T, apply func(from *Server, to *Server, m Message)) {
  if from.messages_net.Len() == 0 {
    t.Skip("no messages to apply")
    return
  }
  msg := from.messages_net.PopFront()
  apply(from, to, msg)
}

func (m *serverClientMachine) SendClientMessage(t *rapid.T) {
  ApplyMessage(&m.client, &m.server, t, func(from *Server, to *Server, m Message) {
    to.HandleMsg(m, t)
  })
}

func (m *serverClientMachine) SendServerResponse(t *rapid.T) {
  ApplyMessage(&m.server, &m.client, t, func(from *Server, to *Server, m Message) {
    to.HandleMsg(m, t)
  })
}

func (m *serverClientMachine) DuplicateClientMessage(t *rapid.T) {
  ApplyMessage(&m.client, &m.server, t, func(from *Server, to *Server, m Message) {
    from.messages_net.PushFront(m)
    from.messages_net.PushFront(m)
  })
}

func (m *serverClientMachine) DelayClientMessage(t *rapid.T) {
  ApplyMessage(&m.client, &m.server, t, func(from *Server, to *Server, m Message) {
    from.messages_net.PushBack(m)
  })
}

func (m *serverClientMachine) DelayServerResponse(t *rapid.T) {
  ApplyMessage(&m.server, &m.client, t, func(from *Server, to *Server, m Message) {
    from.messages_net.PushBack(m)
  })
}

func (m *serverClientMachine) DropClientMessage(t *rapid.T) {
  ApplyMessage(&m.client, &m.server, t, func(from *Server, to *Server, m Message) {
  })
}

func (m *serverClientMachine) DropServerResponse(t *rapid.T) {
  ApplyMessage(&m.server, &m.client, t, func(from *Server, to *Server, m Message) {
  })
}

func (m *serverClientMachine) RetryClientQueries(t *rapid.T) {
  m.client.RetryQueries()
}

func (m *serverClientMachine) ProducePersistentMessage(t *rapid.T) {
  if m.in_sync {
    t.Skip("cannot produce message, sync in progress")
    return
  }
  m.client.ProducePersistentQuery(t)
}

func (m *serverClientMachine) StartSyncing(t *rapid.T) {
  m.in_sync = true
}

func (m *serverClientMachine) ProduceNoise(t *rapid.T) {
  // just incorrect queries produced from nowhere
  m.client.messages_net.PushBack(prepareMsg{msg: queryData{uuid: GenerateUUID()}})
  m.client.messages_net.PushBack(commitMsg{msg: queryData{uuid: GenerateUUID()}, slot: slotMetaData{uuid: GenerateUUID()}})
}

func TestClientServer(t *testing.T) {
  t.Parallel()
  rapid.Check(t, rapid.Run[*serverClientMachine]())
}
