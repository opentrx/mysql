package mysql

type UndoLogParser interface {
	GetName() string

	// return the default content if undo log is empty
	GetDefaultContent() []byte

	Encode(branchUndoLog *branchUndoLog) []byte

	Decode(data []byte) *branchUndoLog
}

func GetUndoLogParser() UndoLogParser {
	return ProtoBufUndoLogParser{}
}
