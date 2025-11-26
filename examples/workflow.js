/**
 * Advanced Workflow Example
 * 
 * Demonstrates:
 * - Job chains
 * - Dependencies
 * - Success/failure handlers
 */

const { Client, Chain, Workflow, Transaction } = require('../../src');

async function main() {
  const client = new Client({
    redis: { host: 'localhost', port: 6379 },
    meshId: 'workflow-mesh',
    serverId: 'workflow-producer',
  });

  await client.init();

  // Example 1: Simple Chain
  console.log('\n=== Example 1: Job Chain ===');
  const chain = new Chain(client);
  const chainJobIds = await chain
    .addJob({
      type: 'validate-order',
      payload: { orderId: '123' },
    })
    .onSuccess({
      type: 'charge-card',
      payload: { orderId: '123', amount: 99.99 },
    })
    .onFailure({
      type: 'send-validation-error',
      payload: { orderId: '123' },
    })
    .execute();

  console.log('Chain created:', chainJobIds);

  // Example 2: Multi-step Workflow
  console.log('\n=== Example 2: Workflow ===');
  const workflow = new Workflow(client, 'order-fulfillment');
  const workflowResult = await workflow
    .addStep('validate', {
      type: 'validate-order',
      payload: { orderId: '456' },
    })
    .addStep('charge', {
      type: 'charge-card',
      payload: { orderId: '456' },
    })
    .addStep('ship', {
      type: 'ship-order',
      payload: { orderId: '456' },
    })
    .execute({ customerId: 'customer-789' });

  console.log('Workflow started:', workflowResult);

  // Check workflow status
  setTimeout(async () => {
    const status = await workflow.getStatus();
    console.log('Workflow status:', status);
  }, 5000);

  // Example 3: Transaction with Saga Pattern
  console.log('\n=== Example 3: Transaction (Saga) ===');
  const transaction = new Transaction(client);
  const txResult = await transaction
    .addAction(
      { type: 'reserve-inventory', payload: { sku: 'PROD-001', qty: 5 } },
      { type: 'release-inventory', payload: { sku: 'PROD-001', qty: 5 } }
    )
    .addAction(
      { type: 'debit-wallet', payload: { userId: 'user-123', amount: 99.99 } },
      { type: 'credit-wallet', payload: { userId: 'user-123', amount: 99.99 } }
    )
    .addAction(
      { type: 'create-shipment', payload: { orderId: '789' } },
      { type: 'cancel-shipment', payload: { orderId: '789' } }
    )
    .execute();

  console.log('Transaction result:', txResult);

  setTimeout(() => {
    client.shutdown();
  }, 10000);
}

main().catch(console.error);
