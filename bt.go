package go_behavior_tree

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/itchyny/gojq"
	"github.com/joeycumines/go-behaviortree"
	"github.com/xlab/treeprint"
)

type DoFunc func(n *Node) (behaviortree.Status, error)

type NodeInstance struct {
	ID          string      `json:"id,omitempty"`
	Root        *Node       `json:"root"`
	Blackboard  interface{} `json:"blackboard"`
	CurrentPath []int       `json:"current_path"`
	dumpFunc    func(ctx context.Context, n *Node) error
	ctx         context.Context
}

type TickType string

var (
	DoFuncDefault = ""
	DoFuncSuccess = "success"
	DoFuncFailure = "failure"
	tickMap       = map[TickType]func(n *Node) behaviortree.Tick{
		TickType(""):                                       TickSequence,
		TickType(funcName(TickAll)):                        TickAll,
		TickType(funcName(TickSequence)):                   TickSequence,
		TickType(funcName(TickRepeatUntilSuccessSequence)): TickRepeatUntilSuccessSequence,
		TickType(funcName(TickSelector)):                   TickSelector,
	}
	doFuncMap = map[string]DoFunc{
		DoFuncDefault: DefaultDoFunc,
		DoFuncSuccess: DefaultDoFuncSuccess,
		DoFuncFailure: DefaultDoFuncFailure,
	}
)

func DefaultDoFunc(n *Node) (behaviortree.Status, error) {
	n.PushOutput("default do func success")
	return behaviortree.Success, nil
}

func DefaultDoFuncFailure(n *Node) (behaviortree.Status, error) {
	n.PushOutput("default do func failure")
	return behaviortree.Failure, nil
}

func DefaultDoFuncSuccess(n *Node) (behaviortree.Status, error) {
	n.PushOutput("default do func success")
	return behaviortree.Success, nil
}

func funcName(fn interface{}) string {
	var name string
	nm := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	arr := strings.Split(nm, ".")
	if len(arr) > 0 {
		name = arr[len(arr)-1]
	}
	return name
}

func DoFuncRegister(doFunc DoFunc) {
	var name string
	nm := runtime.FuncForPC(reflect.ValueOf(doFunc).Pointer()).Name()
	arr := strings.Split(nm, ".")
	if len(arr) > 0 {
		name = arr[len(arr)-1]
	}
	name = strings.TrimRight(name, "-fm")
	doFuncMap[name] = func(n *Node) (behaviortree.Status, error) {
		defer func() {
			if r := recover(); r != nil {
				n.Status = behaviortree.Failure
				stack := debug.Stack()
				n.PushOutput(string(stack))
				n.Dump()
			}
		}()
		return doFunc(n)
	}
}

func (n *Node) GetDoFunc() DoFunc {
	if n.DoFunc != nil {
		return n.DoFunc
	}
	doFunc, ok := doFuncMap[n.DoFuncName]
	if !ok {
		doFunc = DefaultDoFunc
	}
	return doFunc
}

type Node struct {
	btChildren         []behaviortree.Node
	btNode             behaviortree.Node
	instance           *NodeInstance
	preCheckExpression *gojq.Query
	DoFunc             DoFunc                 `json:"-"`
	Tick               behaviortree.Tick      `json:"-"`
	Error              error                  `json:"-"`
	ID                 string                 `json:"id,omitempty"`
	PreCheckExpression string                 `json:"pre_check_expression,omitempty"`
	DoFuncName         string                 `json:"do_func_name,omitempty"`
	TickType           TickType               `json:"tick_type,omitempty"`
	Name               string                 `json:"name,omitempty"`
	Alias              string                 `json:"alias,omitempty"`
	Path               []int                  `json:"path,omitempty"`
	Children           []*Node                `json:"children,omitempty"`
	Output             []string               `json:"output,omitempty"`
	Status             behaviortree.Status    `json:"status,omitempty"`
	Deprecated         bool                   `json:"deprecated,omitempty"`
	LocalBlackboard    map[string]interface{} `json:"local_blackboard,omitempty"`
}

func (n *Node) PreRunCheck() *bool {
	if n.preCheckExpression == nil {
		return nil
	}
	if n.instance.Blackboard == nil {
		return nil
	}
	bts, err := json.Marshal(n.instance.Blackboard)
	if err != nil {
		n.PushOutput(fmt.Sprintf("PreRunCheck error: %s", err.Error()))
		return nil
	}
	var bb map[string]interface{}
	err = json.Unmarshal(bts, &bb)
	if err != nil {
		n.PushOutput(fmt.Sprintf("PreRunCheck error: %s", err.Error()))
		return nil
	}
	iter := n.preCheckExpression.Run(bb)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		run, ok := v.(bool)
		if !ok {
			break
		}
		return &run
	}
	return nil
}

func isAscendingTraversal(pathA, pathB []int) bool {
	minLen := len(pathA)
	if len(pathB) < minLen {
		minLen = len(pathB)
	}

	// find longest common prefix
	commonPrefixLen := 0
	for i := 0; i < minLen; i++ {
		if pathA[i] == pathB[i] {
			commonPrefixLen++
		} else {
			break
		}
	}

	// B is a child path of A
	// {[]int{1, 1}, []int{1, 1, 1}, true}
	if commonPrefixLen == len(pathA) && len(pathB) >= commonPrefixLen {
		return true
	}

	// B is the next path of A's ancestor path
	if len(pathA) > commonPrefixLen && len(pathB) > commonPrefixLen {
		if pathB[commonPrefixLen] > pathA[commonPrefixLen] {
			// {[]int{1, 2, 1}, []int{1, 3}, true}
			return true
		}
	}

	return false
}

func (n *Node) IsDone() bool {
	if len(n.instance.CurrentPath) == 0 || isAscendingTraversal(n.instance.CurrentPath, n.Path) {
		n.instance.CurrentPath = n.Path
	}

	if n.Deprecated {
		n.Status = behaviortree.Success
		return true
	}

	run := n.PreRunCheck()
	if run != nil && !*run {
		// precheck passed, the node should not be executed
		n.Status = behaviortree.Success

		return true
	}

	if n.Status == behaviortree.Success {
		return true
	}
	if n.Status == behaviortree.Failure {
		return false
	}

	return false
}

func (n *Node) GetNodeInstance() *NodeInstance {
	return n.instance
}

func (n *Node) Dump() {
	defer func() {
		if r := recover(); r != nil {
			n.Status = behaviortree.Failure
			stack := debug.Stack()
			n.PushOutput(string(stack))
		}
	}()

	if n.instance.dumpFunc == nil {
		return
	}

	err := n.instance.dumpFunc(context.Background(), n)
	if err != nil {
		return
	}
}

func (ni *NodeInstance) SetDumpFunc(fn func(ctx context.Context, n *Node) error) {
	ni.dumpFunc = fn
}

func (n *Node) GetTick() (behaviortree.Tick, error) {
	tm, ok := tickMap[n.TickType]
	if !ok {
		return nil, errors.New("tick type not exist: " + string(n.TickType))
	}
	return tm(n), nil
}

func TickSelector(n *Node) behaviortree.Tick {
	return func(children []behaviortree.Node) (behaviortree.Status, error) {
		if n.IsDone() {
			return n.Status, n.Error
		}

		defer func() {
			n.Dump()
		}()

		if n.DoFunc != nil {
			n.Status, n.Error = n.DoFunc(n)
			if n.Error != nil {
				return n.Status, n.Error
			}
			if n.Status != behaviortree.Success {
				return n.Status, nil
			}
		}

		n.Status, n.Error = behaviortree.Selector(children)
		return n.Status, n.Error
	}
}

func TickSequence(n *Node) behaviortree.Tick {
	return func(children []behaviortree.Node) (behaviortree.Status, error) {
		if n.IsDone() {
			return n.Status, n.Error
		}

		defer func() {
			n.Dump()
		}()

		if n.DoFunc != nil {
			n.Status, n.Error = n.DoFunc(n)
			if n.Error != nil {
				return n.Status, n.Error
			}
			if n.Status != behaviortree.Success {
				return n.Status, nil
			}
		}

		n.Status, n.Error = behaviortree.Sequence(children)
		return n.Status, n.Error
	}
}

func TickAll(n *Node) behaviortree.Tick {
	return func(children []behaviortree.Node) (behaviortree.Status, error) {
		if n.IsDone() {
			return n.Status, n.Error
		}

		defer func() {
			n.Dump()
		}()

		if n.DoFunc != nil {
			n.Status, n.Error = n.DoFunc(n)
			if n.Error != nil {
				return n.Status, n.Error
			}
			if n.Status != behaviortree.Success {
				return n.Status, nil
			}
		}

		n.Status, n.Error = behaviortree.All(children)
		return n.Status, n.Error
	}
}

func TickRepeatUntilSuccessSequence(n *Node) behaviortree.Tick {
	return func(children []behaviortree.Node) (behaviortree.Status, error) {
		if n.IsDone() {
			return n.Status, n.Error
		}
		defer func() {
			n.Dump()
		}()

		if n.DoFunc != nil {
			n.Status, n.Error = n.DoFunc(n)
			if n.Error != nil {
				return n.Status, n.Error
			}
			if n.Status != behaviortree.Success {
				return n.Status, nil
			}
		}

		n.Status, n.Error = behaviortree.Sequence(children)
		if n.Status != behaviortree.Success {
			// reset child node status
			n.resetChildNodeStatus(0)
			n.Status = behaviortree.Running
			n.Error = nil
		}
		return n.Status, n.Error
	}
}

func (n *Node) GetBTNodes() []behaviortree.Node {
	return n.btChildren
}

func (n *Node) IsRoot() bool {
	if len(n.Path) == 1 && n.Path[0] == 1 {
		return true
	}
	return false
}

func (ni *NodeInstance) BuildBehaviorTree() error {
	var rootPath = []int{1}
	err := ni.Root.buildBehaviorTree(ni, &rootPath)
	return err
}

func (n *Node) buildBehaviorTree(ni *NodeInstance, parentPath *[]int) error {
	var err error
	if n.Name == "" {
		return errors.New("node name is empty")
	}
	n.instance = ni
	n.Path = *parentPath
	n.Tick, err = n.GetTick()
	if err != nil {
		return err
	}

	n.DoFunc = n.GetDoFunc()

	if n.PreCheckExpression != "" {
		n.preCheckExpression, err = gojq.Parse(n.PreCheckExpression)
		if err != nil {
			return err
		}
	}

	for idx, child := range n.Children {
		path := make([]int, len(*parentPath))
		copy(path, *parentPath)
		path = append(path, idx+1)

		child.instance = n.instance
		if err = child.buildBehaviorTree(ni, &path); err != nil {
			return err
		}
		n.btChildren = append(n.btChildren, child.btNode)
	}

	n.btNode = behaviortree.NewNode(n.Tick, n.btChildren)
	return nil
}

func (n *Node) PushOutput(output string) {
	n.Output = append(n.Output, fmt.Sprintf("%v %v", time.Now(), output))
}

func (n *Node) printTreePath(pTree treeprint.Tree) {
	if len(n.Children) == 0 {
		nn := fmt.Sprintf("%v-%v-%v", n.Name, n.Path, n.Status)
		pTree.AddNode(nn)
		return
	}
	nn := fmt.Sprintf("%v-%v-%v", n.Name, n.Path, n.Status)
	bt := pTree.AddBranch(nn)
	for _, child := range n.Children {
		child.printTreePath(bt)
	}
}

func (n *Node) PrintTreePath() {
	tree := treeprint.New()
	n.printTreePath(tree)
	fmt.Println(tree.String())
}

func (ni *NodeInstance) Marshal() ([]byte, error) {
	return json.MarshalIndent(ni, "", "  ")
}

func (n *Node) GetBlackboard() interface{} {
	return n.instance.Blackboard
}

func (ni *NodeInstance) Tick() (behaviortree.Status, error) {
	return ni.Root.Tick(ni.Root.GetBTNodes())
}

func (ni *NodeInstance) PrintTreePath() {
	ni.Root.PrintTreePath()
}

func (ni *NodeInstance) JumpOver(req FindNode) error {
	find, err := ni.Root.Find(req)
	if err != nil {
		return err
	}
	find.PushOutput("jump over")
	find.Status = behaviortree.Success
	return nil
}

func (n *Node) Find(req FindNode) (*Node, error) {
	find := n.find(req)
	if find == nil {
		return nil, errors.New("not found path")
	}
	return find, nil
}

func (n *Node) FindCurrentTickNode() (*Node, error) {
	if len(n.instance.CurrentPath) == 0 {
		return n.instance.Root, nil
	}
	nd, err := n.Find(FindNode{
		Path: n.instance.CurrentPath,
	})
	return nd, err
}

func (n *Node) find(req FindNode) *Node {
	if n.isFind(req) {
		return n
	}
	for _, child := range n.Children {
		find := child.find(req)
		if find != nil {
			return find
		}
	}
	return nil
}

type FindNode struct {
	Name string
	Path []int
}

func (n *Node) isFind(req FindNode) bool {
	if n.Name == req.Name {
		return true
	}
	if len(n.Path) != len(req.Path) {
		return false
	}
	for idx, p := range req.Path {
		if p != n.Path[idx] {
			return false
		}
	}
	return true
}

func (n *Node) resetChildNodeStatus(status behaviortree.Status) {
	for _, child := range n.Children {
		child.Status = status
		child.LocalBlackboard = nil
		child.resetChildNodeStatus(status)
	}
}

func (n *Node) SetLocalBlackboard(key string, value interface{}) {
	if n.LocalBlackboard == nil {
		n.LocalBlackboard = map[string]interface{}{}
	}
	n.LocalBlackboard[key] = value
}

func (n *Node) GetLocalBlackboard(key string) interface{} {
	if n.LocalBlackboard == nil {
		return ""
	}
	return n.LocalBlackboard[key]
}

func (ni *NodeInstance) Retry() error {
	node, err := ni.Root.FindCurrentTickNode()
	if err != nil {
		return err
	}
	if node == nil {
		return nil
	}
	if node.Status == behaviortree.Success {
		// no failure node
		return nil
	}
	node.resetChildNodeStatus(0)
	node.Status = 0
	return nil
}

func (ni *NodeInstance) IsDone() bool {
	return ni.Root.IsDone()
}

func (ni *NodeInstance) WithContext(ctx context.Context) {
	ni.ctx = ctx
}

func (ni *NodeInstance) Context() context.Context {
	return ni.ctx
}
