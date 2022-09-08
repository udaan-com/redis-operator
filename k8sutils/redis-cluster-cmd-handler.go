package k8sutils

import (
	"context"
	"github.com/gorilla/mux"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"net/http"
	redisv1beta1 "redis-operator/api/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func recoverCluster(ns string, cn string, cl client.Client) error {
	logger := generateRedisManagerLogger(ns, cn)
	cr := &redisv1beta1.RedisCluster{}
	if err := cl.Get(context.TODO(), types.NamespacedName{Name: cn, Namespace: ns}, cr); err != nil {
		logger.Error(err, "CRD fetch error")
		return err
	}
	err := ExecuteGracefulFailOverOperation(cr)
	if err != nil {
		logger.Error(err, "graceful failover error")
	}
	return err
}

func forceRecoverCluster(ns string, cn string, cl client.Client) error {
	logger := generateRedisManagerLogger(ns, cn)
	cr := &redisv1beta1.RedisCluster{}
	if err := cl.Get(context.TODO(), types.NamespacedName{Name: cn, Namespace: ns}, cr); err != nil {
		logger.Error(err, "CRD fetch error")
		return err
	}
	err := ExecuteFailoverOperation(cr)
	if err != nil {
		logger.Error(err, "graceful failover error")
	}
	return err
}

type RedisClusterCmdHandler struct {
	client.Client
}

func send(w http.ResponseWriter, res map[string]string, status int) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(res)
}

func sendError(w http.ResponseWriter, res error, status int) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(res)
}
func (h *RedisClusterCmdHandler) resetClusterHandler(w http.ResponseWriter, r *http.Request) {
	ns := mux.Vars(r)["namespace"]
	cn := mux.Vars(r)["clusterName"]
	err := recoverCluster(ns, cn, h.Client)
	if err != nil {
		sendError(w, err, http.StatusInternalServerError)
		return
	}
	send(w, map[string]string{"status": "OK"}, http.StatusOK)
}

func (h *RedisClusterCmdHandler) forceResetClusterHandler(w http.ResponseWriter, r *http.Request) {
	ns := mux.Vars(r)["namespace"]
	cn := mux.Vars(r)["clusterName"]
	err := forceRecoverCluster(ns, cn, h.Client)
	if err != nil {
		sendError(w, err, http.StatusInternalServerError)
		return
	}
	send(w, map[string]string{"status": "OK"}, http.StatusOK)
}

func (h *RedisClusterCmdHandler) StartCmdServer() {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/cluster/{namespace}/{clusterName}/reset", h.resetClusterHandler).Methods("POST")
	router.HandleFunc("/cluster/{namespace}/{clusterName}/force-reset", h.forceResetClusterHandler).Methods("POST")
	http.ListenAndServe(":8090", router)
}
