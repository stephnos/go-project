package orderfulfillment

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spapa/orchid/pkg/workflow"
)

type Input struct {
	OrderID              string `json:"order_id"`
	CustomerID           string `json:"customer_id"`
	SKU                  string `json:"sku"`
	Quantity             int    `json:"quantity"`
	ForcePaymentFailure  bool   `json:"force_payment_failure"`
	ForceShipmentFailure bool   `json:"force_shipment_failure"`
}

func Register(registry *workflow.Registry) error {
	builder := workflow.New("order_fulfillment")
	builder.Activity("validate_order", "orders.validate",
		workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 1}),
	)
	builder.Activity("reserve_inventory", "inventory.reserve",
		workflow.DependsOn("validate_order"),
		workflow.WithQueue("operations"),
		workflow.WithCompensation("inventory.release", "operations"),
	)
	builder.Activity("charge_payment", "payments.charge",
		workflow.DependsOn("validate_order"),
		workflow.WithQueue("payments"),
		workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 4, InitialBackoff: 250 * time.Millisecond, MaxBackoff: 2 * time.Second}),
		workflow.WithCompensation("payments.refund", "payments"),
	)
	builder.Timer("fraud_window", 400*time.Millisecond, workflow.DependsOn("charge_payment"))
	builder.Activity("pack_items", "warehouse.pack",
		workflow.DependsOn("reserve_inventory", "charge_payment"),
		workflow.WithQueue("operations"),
	)
	builder.Activity("dispatch_shipment", "shipping.dispatch",
		workflow.DependsOn("pack_items", "fraud_window"),
		workflow.WithQueue("shipping"),
		workflow.WithCompensation("shipping.recall", "shipping"),
	)
	builder.Activity("send_confirmation", "customer.notify",
		workflow.DependsOn("dispatch_shipment"),
		workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 2, InitialBackoff: 150 * time.Millisecond, MaxBackoff: 400 * time.Millisecond}),
	)

	definition, err := builder.Build()
	if err != nil {
		return err
	}
	registry.MustRegister(definition)

	registry.MustRegisterActivity("orders.validate", func(ctx context.Context, input workflow.ActivityInput) (any, error) {
		order, err := decode(input.RunInput)
		if err != nil {
			return nil, err
		}
		if order.Quantity <= 0 || order.OrderID == "" || order.SKU == "" {
			return nil, workflow.NonRetryable(fmt.Errorf("invalid order payload"))
		}
		return map[string]any{
			"validated": true,
			"order_id":  order.OrderID,
		}, nil
	})

	registry.MustRegisterActivity("inventory.reserve", func(ctx context.Context, input workflow.ActivityInput) (any, error) {
		order, err := decode(input.RunInput)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"reservation_id": "res-" + order.OrderID,
			"sku":            order.SKU,
			"quantity":       order.Quantity,
		}, nil
	})

	registry.MustRegisterActivity("inventory.release", func(ctx context.Context, input workflow.ActivityInput) (any, error) {
		order, err := decode(input.RunInput)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"released": true,
			"order_id": order.OrderID,
		}, nil
	})

	registry.MustRegisterActivity("payments.charge", func(ctx context.Context, input workflow.ActivityInput) (any, error) {
		order, err := decode(input.RunInput)
		if err != nil {
			return nil, err
		}
		if order.ForcePaymentFailure {
			return nil, fmt.Errorf("payment processor timeout")
		}
		return map[string]any{
			"payment_id": "pay-" + order.OrderID,
			"amount":     order.Quantity * 42,
			"captured":   true,
		}, nil
	})

	registry.MustRegisterActivity("payments.refund", func(ctx context.Context, input workflow.ActivityInput) (any, error) {
		order, err := decode(input.RunInput)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"refunded": true,
			"order_id": order.OrderID,
		}, nil
	})

	registry.MustRegisterActivity("warehouse.pack", func(ctx context.Context, input workflow.ActivityInput) (any, error) {
		order, err := decode(input.RunInput)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"packed":   true,
			"order_id": order.OrderID,
		}, nil
	})

	registry.MustRegisterActivity("shipping.dispatch", func(ctx context.Context, input workflow.ActivityInput) (any, error) {
		order, err := decode(input.RunInput)
		if err != nil {
			return nil, err
		}
		if order.ForceShipmentFailure {
			return nil, workflow.NonRetryable(fmt.Errorf("carrier rejected dispatch"))
		}
		return map[string]any{
			"tracking_id": "trk-" + order.OrderID,
			"carrier":     "orchid-express",
		}, nil
	})

	registry.MustRegisterActivity("shipping.recall", func(ctx context.Context, input workflow.ActivityInput) (any, error) {
		order, err := decode(input.RunInput)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"recalled": true,
			"order_id": order.OrderID,
		}, nil
	})

	registry.MustRegisterActivity("customer.notify", func(ctx context.Context, input workflow.ActivityInput) (any, error) {
		order, err := decode(input.RunInput)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"sent":     true,
			"customer": order.CustomerID,
		}, nil
	})

	return nil
}

func ExampleInput() Input {
	return Input{
		OrderID:    "ord-1001",
		CustomerID: "cust-42",
		SKU:        "sku-7",
		Quantity:   3,
	}
}

func decode(raw json.RawMessage) (Input, error) {
	var input Input
	err := json.Unmarshal(raw, &input)
	return input, err
}
