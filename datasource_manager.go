package mysql

import (
	"context"
	"fmt"
	"sync"

	"github.com/opentrx/seata-golang/v2/pkg/apis"
	"github.com/opentrx/seata-golang/v2/pkg/client/base/model"
	"github.com/opentrx/seata-golang/v2/pkg/util/log"
)

var dataSourceManager DataSourceManager

type DataSourceManager struct {
	ResourceCache map[string]*connector
	connections   map[string]*mysqlConn
	sync.Mutex
}

func init() {
	dataSourceManager = DataSourceManager{
		ResourceCache: make(map[string]*connector),
		connections:   make(map[string]*mysqlConn),
	}
}

func GetDataSourceManager() DataSourceManager {
	return dataSourceManager
}

func RegisterResource(dsn string) {
	cfg, err := ParseDSN(dsn)
	if err == nil {
		c := &connector{
			cfg: cfg,
		}
		dataSourceManager.ResourceCache[c.cfg.DBName] = c
		InitTableMetaCache(c.cfg.DBName)
	}
}

func (resourceManager DataSourceManager) GetConnection(resourceID string) *mysqlConn {
	conn, ok := resourceManager.connections[resourceID]
	if ok && conn.IsValid() {
		return conn
	}
	resourceManager.Lock()
	defer resourceManager.Unlock()
	conn, ok = resourceManager.connections[resourceID]
	if ok && conn.IsValid() {
		return conn
	}
	db := resourceManager.ResourceCache[resourceID]
	connection, err := db.Connect(context.Background())
	if err != nil {
		log.Error(err)
	}
	conn = connection.(*mysqlConn)
	resourceManager.connections[resourceID] = conn
	return conn
}

func (resourceManager DataSourceManager) BranchCommit(ctx context.Context, request *apis.BranchCommitRequest) (*apis.BranchCommitResponse, error) {
	db := resourceManager.ResourceCache[request.ResourceID]
	if db == nil {
		log.Errorf("Data resource is not exist, resourceID: %s", request.ResourceID)
		return &apis.BranchCommitResponse{
			ResultCode: apis.ResultCodeFailed,
			Message:    fmt.Sprintf("Data resource is not exist, resourceID: %s", request.ResourceID),
		}, nil
	}

	//todo 改为异步批量操作
	undoLogManager := GetUndoLogManager()
	conn := resourceManager.GetConnection(request.ResourceID)

	if conn == nil || !conn.IsValid() {
		return &apis.BranchCommitResponse{
			ResultCode: apis.ResultCodeFailed,
			Message:    "Connection is not valid",
		}, nil
	}
	err := undoLogManager.DeleteUndoLog(conn, request.XID, request.BranchID)
	if err != nil {
		log.Errorf("[stacktrace]branchCommit failed. xid:[%s], branchID:[%d], resourceID:[%s], branchType:[%d], applicationData:[%v]",
			request.XID, request.BranchID, request.ResourceID, request.BranchType, request.ApplicationData)
		log.Error(err)
		return &apis.BranchCommitResponse{
			ResultCode: apis.ResultCodeFailed,
			Message:    err.Error(),
		}, nil
	}
	return &apis.BranchCommitResponse{
		ResultCode:   apis.ResultCodeSuccess,
		XID:          request.XID,
		BranchID:     request.BranchID,
		BranchStatus: apis.PhaseTwoCommitted,
	}, nil
}

func (resourceManager DataSourceManager) BranchRollback(ctx context.Context, request *apis.BranchRollbackRequest) (*apis.BranchRollbackResponse, error) {
	db := resourceManager.ResourceCache[request.ResourceID]
	if db == nil {
		return &apis.BranchRollbackResponse{
			ResultCode: apis.ResultCodeFailed,
			Message:    fmt.Sprintf("Data resource is not exist, resourceID: %s", request.ResourceID),
		}, nil
	}

	//todo 使用前镜数据覆盖当前数据
	undoLogManager := GetUndoLogManager()
	conn, err := db.Connect(context.Background())
	defer conn.Close()
	if err != nil {
		log.Error(err)
		return &apis.BranchRollbackResponse{
			ResultCode:   apis.ResultCodeSuccess,
			XID:          request.XID,
			BranchID:     request.BranchID,
			BranchStatus: apis.PhaseTwoRollbackFailedRetryable,
		}, nil
	}
	c := conn.(*mysqlConn)
	err = undoLogManager.Undo(c, request.XID, request.BranchID, db.cfg.DBName)
	if err != nil {
		log.Errorf("[stacktrace]branchRollback failed. xid:[%s], branchID:[%d], resourceID:[%s], branchType:[%d], applicationData:[%v]",
			request.XID, request.BranchID, request.ResourceID, request.BranchType, request.ApplicationData)
		log.Error(err)
		return &apis.BranchRollbackResponse{
			ResultCode:   apis.ResultCodeSuccess,
			XID:          request.XID,
			BranchID:     request.BranchID,
			BranchStatus: apis.PhaseTwoRollbackFailedRetryable,
		}, nil
	}
	return &apis.BranchRollbackResponse{
		ResultCode:   apis.ResultCodeSuccess,
		XID:          request.XID,
		BranchID:     request.BranchID,
		BranchStatus: apis.PhaseTwoRolledBack,
	}, nil
}

func (resourceManager DataSourceManager) RegisterResource(resource model.Resource) {
	if connector, ok := resource.(*connector); ok {
		resourceManager.ResourceCache[resource.GetResourceID()] = connector
	}
}

func (resourceManager DataSourceManager) UnregisterResource(resource model.Resource) {
	delete(resourceManager.ResourceCache, resource.GetResourceID())
}

func (resourceManager DataSourceManager) GetBranchType() apis.BranchSession_BranchType {
	return apis.AT
}
