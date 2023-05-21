// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"math"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-community/promql-engine/execution/model"
)

type scalarFunctionOperator struct {
	stepsBatch  int
	currentStep int64
	mint        int64
	maxt        int64
	step        int64
	pool        *model.VectorPool
	next        model.VectorOperator
}

func (o *scalarFunctionOperator) Explain() (me string, next []model.VectorOperator) {
	return "[*scalarFunctionOperator]", []model.VectorOperator{}
}

func (o *scalarFunctionOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	return nil, nil
}

func (o *scalarFunctionOperator) GetPool() *model.VectorPool {
	return o.pool
}

func (o *scalarFunctionOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if o.currentStep > o.maxt {
		return nil, nil
	}

	in, err := o.next.Next(ctx)
	if err != nil {
		return nil, err
	}

	result := o.GetPool().GetVectorBatch()
	if len(in) == 0 {
		for i := 0; i < o.stepsBatch && o.currentStep <= o.maxt; i++ {
			sv := o.GetPool().GetStepVector(o.currentStep)
			sv.AppendSample(o.GetPool(), 0, math.NaN())
			result = append(result, sv)
			o.currentStep += o.step
		}
	} else {
		for _, vector := range in {
			sv := o.GetPool().GetStepVector(o.currentStep)
			if len(vector.Samples) != 1 {
				sv.AppendSample(o.GetPool(), 0, math.NaN())
			} else {
				sv.AppendSample(o.GetPool(), 0, vector.Samples[0])
			}
			result = append(result, sv)
			o.currentStep += o.step
			o.next.GetPool().PutStepVector(vector)
		}
	}
	o.next.GetPool().PutVectors(in)
	return result, nil
}
