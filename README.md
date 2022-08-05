# Tiny Redis
Implementation if redis-like database in Go. Tiny Redis uses architecture similar to Redis: persistent append only file + snapshots.

# Todolist
- [x] Complete support for GET, SET commands
- [x] TTL support for SET
- [x] Support for DEL
- [] Snapshots
- [] Support all commands for string datatype
- [] Support for advanced data types
- [] Add cleaning of expired keys
- [] Integration tests
- [] Integration tests that compare semantics to redis server