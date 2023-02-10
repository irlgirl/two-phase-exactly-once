package exactly_once

// UUID
var uuid_cnt int

func GenerateUUID() int {
  uuid_cnt += 1
  return uuid_cnt
}
///

