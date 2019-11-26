package migrators

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/pager"
	"k8s.io/klog"
)

const (
	defaultConcurrency = 10
)

// WorkerFunc function that is executed by workers to process a single item
type WorkerFunc func(*unstructured.Unstructured) error

// ListProcessor represents a type that processes resources in parallel.
// It retrieves resources from the server in batches and distributes among set of workers.
type ListProcessor struct {
	concurrency   int
	workerFn      WorkerFunc
	dynamicClient dynamic.Interface
	ctx           context.Context
}

// NewListProcessor creates a new instance of ListProcessor
func NewListProcessor(ctx context.Context, dynamicClient dynamic.Interface, workerFn WorkerFunc) *ListProcessor {
	return &ListProcessor{
		concurrency:   defaultConcurrency,
		workerFn:      workerFn,
		dynamicClient: dynamicClient,
		ctx:           ctx,
	}
}

// Run starts processing all the instance of the given GVR in batches.
// Note that this operation block until all resources have been process, we can't get the next page or the context has been cancelled
func (p *ListProcessor) Run(gvr schema.GroupVersionResource) error {
	var errs []error
	listPager := pager.New(pager.SimplePageFunc(func(opts metav1.ListOptions) (runtime.Object, error) {
		for {
			allResource, err := p.dynamicClient.Resource(gvr).List(opts)
			if err != nil {
				klog.Warningf("List of %v failed: %v", gvr, err)
				if errors.IsResourceExpired(err) {
					token, err := inconsistentContinueToken(err)
					if err != nil {
						return nil, err
					}
					opts.Continue = token
					klog.V(4).Infof("Relisting %v after handling expired token", gvr)
					continue
				} else if retryable := canRetry(err); retryable == nil || *retryable == false {
					return nil, err // not retryable or we don't know. Return error and controller will restart migration.
				} else {
					if seconds, delay := errors.SuggestsClientDelay(err); delay {
						time.Sleep(time.Duration(seconds) * time.Second)
					}
					klog.V(4).Infof("Relisting %v after retryable error: %v", gvr, err)
					continue
				}
			}

			migrationStarted := time.Now()
			klog.V(4).Infof("Migrating %d objects of %v", len(allResource.Items), gvr)
			err = p.processList(allResource)
			if err != nil {
				errs = append(errs, err)
			}
			klog.V(4).Infof("Migration of %d objects of %v finished in %v", len(allResource.Items), gvr, time.Now().Sub(migrationStarted))

			allResource.Items = nil // do not accumulate items, this fakes the visitor pattern
			return allResource, nil // leave the rest of the list intact to preserve continue token
		}
	}))

	migrationStarted := time.Now()
	listPager.FullListIfExpired = false // prevent memory explosion from full list
	_, listErr := listPager.List(p.ctx, metav1.ListOptions{})
	klog.V(4).Infof("Migration for %v finished in %v", gvr, time.Now().Sub(migrationStarted))
	errs = append(errs, listErr)
	return utilerrors.NewAggregate(errs)
}

func (p *ListProcessor) processList(l *unstructured.UnstructuredList) error {
	workCh := make(chan *unstructured.Unstructured, p.concurrency)
	onWorkerErrorCtx, onWorkerErrorCancel := context.WithCancel(context.Background())
	defer onWorkerErrorCancel()

	go func() {
		defer utilruntime.HandleCrash()
		defer close(workCh)
		for i := range l.Items {
			select {
			case workCh <- &l.Items[i]:
			case <-p.ctx.Done():
				return
			case <-onWorkerErrorCtx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(p.concurrency)
	errCh := make(chan error)
	for i := 0; i < p.concurrency; i++ {
		go func() {
			defer utilruntime.HandleCrash()
			defer wg.Done()
			p.worker(workCh, errCh)
		}()
	}

	go func() {
		defer utilruntime.HandleCrash()
		wg.Wait()
		close(errCh)
	}()

	var errors []error
	for err := range errCh {
		errors = append(errors, err)
		onWorkerErrorCancel()
	}
	return utilerrors.NewAggregate(errors)
}

func (p *ListProcessor) worker(workCh <-chan *unstructured.Unstructured, errCh chan<- error) {
	for item := range workCh {
		err := executeWorkerFunc(p.workerFn, item)
		if err == nil {
			continue
		}
		select {
		case errCh <- err:
			continue
		case <-p.ctx.Done():
			return
		}
	}
}

func executeWorkerFunc(workerFn WorkerFunc, obj *unstructured.Unstructured) (result error) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				result = err
			} else {
				result = fmt.Errorf("panic: %v", r)
			}
		}
	}()
	result = workerFn(obj)
	return
}

// inconsistentContinueToken extracts the continue token from the response which might be used to retrieve the remainder of the results
//
// Note:
// continuing with the provided token might result in an inconsistent list. Objects that were created,
// modified, or deleted between the time the first chunk was returned and now may show up in the list.
func inconsistentContinueToken(err error) (string, error) {
	status, ok := err.(errors.APIStatus)
	if !ok {
		return "", fmt.Errorf("expected error to implement the APIStatus interface, got %v", reflect.TypeOf(err))
	}
	token := status.Status().ListMeta.Continue
	if len(token) == 0 {
		return "", fmt.Errorf("expected non empty continue token")
	}
	return token, nil
}
