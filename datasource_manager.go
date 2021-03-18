package mysql

import (
	"context"
	"fmt"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/transaction-wg/seata-golang/pkg/base/meta"
	"github.com/transaction-wg/seata-golang/pkg/base/protocal"
	"github.com/transaction-wg/seata-golang/pkg/client/config"
	"github.com/transaction-wg/seata-golang/pkg/client/rpc_client"
	"github.com/transaction-wg/seata-golang/pkg/util/log"
)

var (
	DBKEYS_SPLIT_CHAR = ","
)

type DataSourceManager struct {
	RpcClient     *rpc_client.RpcRemoteClient
	ResourceCache map[string]*connector
}

var dataSourceManager DataSourceManager

func InitDataResourceManager() {
	dataSourceManager = DataSourceManager{
		RpcClient:     rpc_client.GetRpcRemoteClient(),
		ResourceCache: make(map[string]*connector),
	}
	go dataSourceManager.handleRegisterRM()
	go dataSourceManager.handleBranchCommit()
	go dataSourceManager.handleBranchRollback()
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

func (resourceManager DataSourceManager) BranchRegister(branchType meta.BranchType, resourceID string,
	clientID string, xid string, applicationData []byte, lockKeys string) (int64, error) {
	request := protocal.BranchRegisterRequest{
		XID:             xid,
		BranchType:      branchType,
		ResourceID:      resourceID,
		LockKey:         lockKeys,
		ApplicationData: applicationData,
	}
	resp, err := resourceManager.RpcClient.SendMsgWithResponse(request)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	response := resp.(protocal.BranchRegisterResponse)
	if response.ResultCode == protocal.ResultCodeSuccess {
		return response.BranchID, nil
	} else {
		return 0, response.GetError()
	}
}

func (resourceManager DataSourceManager) BranchReport(branchType meta.BranchType, xid string, branchID int64,
	status meta.BranchStatus, applicationData []byte) error {
	request := protocal.BranchReportRequest{
		XID:             xid,
		BranchID:        branchID,
		Status:          status,
		ApplicationData: applicationData,
	}
	resp, err := resourceManager.RpcClient.SendMsgWithResponse(request)
	if err != nil {
		return errors.WithStack(err)
	}
	response := resp.(protocal.BranchReportResponse)
	if response.ResultCode == protocal.ResultCodeFailed {
		return response.GetError()
	}
	return nil
}

func (resourceManager DataSourceManager) LockQuery(branchType meta.BranchType, resourceID string, xid string,
	lockKeys string) (bool, error) {
	request := protocal.GlobalLockQueryRequest{
		BranchRegisterRequest: protocal.BranchRegisterRequest{
			XID:        xid,
			ResourceID: resourceID,
			LockKey:    lockKeys,
		}}

	var response protocal.GlobalLockQueryResponse
	resp, err := resourceManager.RpcClient.SendMsgWithResponse(request)
	if err != nil {
		return false, errors.WithStack(err)
	}
	response = resp.(protocal.GlobalLockQueryResponse)

	if response.ResultCode == protocal.ResultCodeFailed {
		return false, errors.Errorf("Response[ %s ]", response.Msg)
	}
	return response.Lockable, nil
}

func (resourceManager DataSourceManager) BranchCommit(branchType meta.BranchType, xid string, branchID int64,
	resourceID string, applicationData []byte) (meta.BranchStatus, error) {
	//todo 改为异步批量操作
	undoLogManager := GetUndoLogManager()
	db := resourceManager.getDB(resourceID)
	conn, err := db.Connect(context.Background())
	defer conn.Close()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	c := conn.(*mysqlConn)
	err = undoLogManager.DeleteUndoLog(c, xid, branchID)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return meta.BranchStatusPhasetwoCommitted, nil
}

func (resourceManager DataSourceManager) BranchRollback(branchType meta.BranchType, xid string, branchID int64,
	resourceID string, applicationData []byte) (meta.BranchStatus, error) {
	//todo 使用前镜数据覆盖当前数据
	undoLogManager := GetUndoLogManager()
	db := resourceManager.getDB(resourceID)
	conn, err := db.Connect(context.Background())
	defer conn.Close()
	if err != nil {
		log.Error(err)
		return meta.BranchStatusPhasetwoCommitFailedRetryable, nil
	}
	c := conn.(*mysqlConn)
	err = undoLogManager.Undo(c, xid, branchID, db.cfg.DBName)
	if err != nil {
		log.Errorf("[stacktrace]branchRollback failed. branchType:[%d], xid:[%s], branchID:[%d], resourceID:[%s], applicationData:[%v]",
			branchType, xid, branchID, resourceID, applicationData)
		log.Error(err)
		return meta.BranchStatusPhasetwoCommitFailedRetryable, nil
	}
	return meta.BranchStatusPhasetwoRollbacked, nil
}

func (resourceManager DataSourceManager) GetBranchType() meta.BranchType {
	return meta.BranchTypeAT
}

func (resourceManager DataSourceManager) getDB(resourceID string) *connector {
	resource := resourceManager.ResourceCache[resourceID]
	return resource
}

func (resourceManager DataSourceManager) handleRegisterRM() {
	for {
		serverAddress := <-resourceManager.RpcClient.GettySessionOnOpenChannel
		resourceManager.doRegisterResource(serverAddress)
	}
}

func (resourceManager DataSourceManager) handleBranchCommit() {
	for {
		rpcRMMessage := <-resourceManager.RpcClient.BranchCommitRequestChannel
		rpcMessage := rpcRMMessage.RpcMessage
		serviceAddress := rpcRMMessage.ServerAddress

		req := rpcMessage.Body.(protocal.BranchCommitRequest)
		resp := resourceManager.doBranchCommit(req)
		resourceManager.RpcClient.SendResponse(rpcMessage, serviceAddress, resp)
	}
}

func (resourceManager DataSourceManager) handleBranchRollback() {
	for {
		rpcRMMessage := <-resourceManager.RpcClient.BranchRollbackRequestChannel
		rpcMessage := rpcRMMessage.RpcMessage
		serviceAddress := rpcRMMessage.ServerAddress

		req := rpcMessage.Body.(protocal.BranchRollbackRequest)
		resp := resourceManager.doBranchRollback(req)
		resourceManager.RpcClient.SendResponse(rpcMessage, serviceAddress, resp)
	}
}

func (resourceManager DataSourceManager) doRegisterResource(serverAddress string) {
	if resourceManager.ResourceCache == nil || len(resourceManager.ResourceCache) == 0 {
		return
	}
	message := protocal.RegisterRMRequest{
		AbstractIdentifyRequest: protocal.AbstractIdentifyRequest{
			Version:                 config.GetClientConfig().SeataVersion,
			ApplicationID:           config.GetClientConfig().ApplicationID,
			TransactionServiceGroup: config.GetClientConfig().TransactionServiceGroup,
		},
		ResourceIDs: resourceManager.getMergedResourceKeys(),
	}

	resourceManager.RpcClient.RegisterResource(serverAddress, message)
}

func (resourceManager DataSourceManager) getMergedResourceKeys() string {
	var builder strings.Builder
	if resourceManager.ResourceCache != nil && len(resourceManager.ResourceCache) > 0 {
		for key, _ := range resourceManager.ResourceCache {
			builder.WriteString(key)
			builder.WriteString(DBKEYS_SPLIT_CHAR)
		}
		resourceKeys := builder.String()
		resourceKeys = resourceKeys[:len(resourceKeys)-1]
		return resourceKeys
	}
	return ""
}

func (resourceManager DataSourceManager) doBranchCommit(request protocal.BranchCommitRequest) protocal.BranchCommitResponse {
	var resp = protocal.BranchCommitResponse{}

	log.Infof("Branch committing: %s %d %s %s", request.XID, request.BranchID, request.ResourceID, request.ApplicationData)
	status, err := resourceManager.BranchCommit(request.BranchType, request.XID, request.BranchID, request.ResourceID, request.ApplicationData)
	if err != nil {
		resp.ResultCode = protocal.ResultCodeFailed
		var trxException *meta.TransactionException
		if errors.As(err, &trxException) {
			resp.TransactionExceptionCode = trxException.Code
			resp.Msg = fmt.Sprintf("TransactionException[%s]", err.Error())
			log.Errorf("Catch TransactionException while do RPC, request: %v", request)
			return resp
		}
		resp.Msg = fmt.Sprintf("RuntimeException[%s]", err.Error())
		log.Errorf("Catch RuntimeException while do RPC, request: %v", request)
		return resp
	}
	resp.XID = request.XID
	resp.BranchID = request.BranchID
	resp.BranchStatus = status
	resp.ResultCode = protocal.ResultCodeSuccess
	return resp
}

func (resourceManager DataSourceManager) doBranchRollback(request protocal.BranchRollbackRequest) protocal.BranchRollbackResponse {
	var resp = protocal.BranchRollbackResponse{}

	log.Infof("Branch rollbacking: %s %d %s", request.XID, request.BranchID, request.ResourceID)
	status, err := resourceManager.BranchRollback(request.BranchType, request.XID, request.BranchID, request.ResourceID, request.ApplicationData)
	if err != nil {
		resp.ResultCode = protocal.ResultCodeFailed
		var trxException *meta.TransactionException
		if errors.As(err, &trxException) {
			resp.TransactionExceptionCode = trxException.Code
			resp.Msg = fmt.Sprintf("TransactionException[%s]", err.Error())
			log.Errorf("Catch TransactionException while do RPC, request: %v", request)
			return resp
		}
		resp.Msg = fmt.Sprintf("RuntimeException[%s]", err.Error())
		log.Errorf("Catch RuntimeException while do RPC, request: %v", request)
		return resp
	}
	resp.XID = request.XID
	resp.BranchID = request.BranchID
	resp.BranchStatus = status
	resp.ResultCode = protocal.ResultCodeSuccess
	return resp
}
