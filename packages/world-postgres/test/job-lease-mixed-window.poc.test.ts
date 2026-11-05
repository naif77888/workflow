import { type ChildProcess, spawn } from 'node:child_process';
import { once } from 'node:events';
import { readFile } from 'node:fs/promises';
import { createServer, type Server, type ServerResponse } from 'node:http';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { getQueueTopicPrefix } from '@workflow/world';
import { makeWorkerUtils } from 'graphile-worker';
import { Pool } from 'pg';
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  test,
  vi,
} from 'vitest';
import { createQueue, type PostgresQueue } from '../src/queue.js';

/**
 * PoC: `confirm()` skips its fencing query when the lock was last shown to be
 * ours within `staleSeconds / 2` of *this* process's window. A peer with a
 * shorter explicit window (`'all'` scope) can release the job inside that
 * span, so the unfenced Graphile acknowledgement deletes the successor's row.
 *
 * Host A (this process): default policy, window set through the test hook.
 * Its delivery has a synchronous stretch of 6s, during which its renewal
 * timer cannot fire.
 * Host B (child process): explicit `jobLockStaleSeconds`. It renews too, so
 * the `'all'` scope's operator assertion ("every process renews") holds.
 *
 * - Vulnerable case: A 60s (confirm window 30s), B 1s. A acknowledges without
 *   a check, the row disappears while B still runs the delivery, and B's
 *   failure is never retried: the message is lost.
 * - Control: A 2s, B 2s, the same 6s stretch. A's fence notices the lost lock
 *   and does not acknowledge; B's failure is retried.
 */
describe.skipIf(process.platform === 'win32')(
  'PoC: unfenced acknowledgement across hosts with different stale windows',
  () => {
    const SYNC_STRETCH_MS = 6_000;
    let container: Awaited<ReturnType<PostgreSqlContainer['start']>>;
    let connectionString: string;
    let pool: Pool;
    let server: Server;
    let queueA: PostgresQueue | undefined;
    let hostB: ChildProcess | undefined;
    let hostBLines: string[] = [];
    let onDeliveryA: (response: ServerResponse) => void = (response) =>
      response.end('{}');
    let deliveriesA: string[] = [];

    async function jobRows() {
      return (
        await pool.query<{ id: string; attempts: number; locked_by: string }>(
          'SELECT id, attempts, locked_by FROM graphile_worker.jobs ORDER BY id'
        )
      ).rows;
    }

    async function waitForHostB(pattern: RegExp, timeoutMs = 15_000) {
      const deadline = Date.now() + timeoutMs;
      while (!hostBLines.some((line) => pattern.test(line))) {
        if (Date.now() > deadline) {
          throw new Error(
            `Host B never printed ${pattern}; it printed: ${JSON.stringify(hostBLines)}`
          );
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }

    async function startHostB(staleSeconds: number) {
      const child = spawn(
        process.execPath,
        [
          fileURLToPath(
            new URL('./fixtures/short-window-host.mjs', import.meta.url)
          ),
        ],
        {
          env: {
            ...process.env,
            DEBUG: '',
            WORKFLOW_POSTGRES_URL: connectionString,
            SHORT_WINDOW_STALE_SECONDS: String(staleSeconds),
          },
          stdio: ['pipe', 'pipe', 'inherit'],
        }
      );
      hostB = child;
      if (!child.stdout) throw new Error('Expected piped stdout');
      createInterface({ input: child.stdout }).on('line', (line) =>
        hostBLines.push(line)
      );
      await waitForHostB(/^listening$/);
      return child;
    }

    /**
     * Queue one message on host A. Its first delivery starts host B's runner
     * and then blocks A's event loop for {@link SYNC_STRETCH_MS} before
     * answering 200; later deliveries to A are answered at once.
     */
    async function runScenario(staleSecondsA: number) {
      const deliveredToA = Promise.withResolvers<void>();
      onDeliveryA = (response) => {
        if (deliveriesA.length > 1) {
          response.end('{}');
          return;
        }
        deliveredToA.resolve();
        // Start the peer only now, so that A claims the job.
        hostB!.stdin!.write('start\n');
        void waitForHostB(/^ready$/).then(() => {
          // A synchronous stretch in A (e.g. an inline step): A's renewal
          // timer cannot fire, but B, another process, keeps sweeping.
          const until = Date.now() + SYNC_STRETCH_MS;
          while (Date.now() < until) {}
          response.end('{}');
        });
      };
      queueA = createQueue(
        {
          connectionString,
          queueConcurrency: 1,
          applicationManagedShutdown: true,
        },
        pool,
        { defaultJobLockStaleSeconds: staleSecondsA }
      );
      await queueA.start();
      await queueA.queue(`${getQueueTopicPrefix('workflow')}test`, {
        runId: 'wrun_mixed_window',
      } as never);
      await deliveredToA.promise;
      const [claimedByA] = await jobRows();
      expect(claimedByA).toMatchObject({ attempts: 1 });
      // B released A's job during the stretch and redelivered it.
      await waitForHostB(/^claimed 2$/);
      expect(deliveriesA).toEqual(['1']);
    }

    const lostLockWarned = (warn: ReturnType<typeof vi.spyOn>) =>
      warn.mock.calls.some((args) =>
        String(args[0]).includes('Lost the lock on Graphile job')
      );

    beforeAll(async () => {
      delete process.env.WORKFLOW_POSTGRES_JOB_LOCK_STALE_SECONDS;
      container = await new PostgreSqlContainer('postgres:15-alpine').start();
      connectionString = container.getConnectionUri();
      pool = new Pool({ connectionString, max: 10 });
      const utils = await makeWorkerUtils({ pgPool: pool });
      await utils.migrate();
      await utils.release();
      await pool.query('CREATE SCHEMA IF NOT EXISTS workflow');
      await pool.query(
        await readFile(
          new URL(
            '../src/drizzle/migrations/0030_add_job_lock_holders.sql',
            import.meta.url
          ),
          'utf8'
        )
      );
      server = createServer(async (request, response) => {
        await request.toArray();
        deliveriesA.push(String(request.headers['x-vqs-message-attempt']));
        onDeliveryA(response);
      });
      await new Promise<void>((resolve, reject) => {
        server.once('error', reject);
        server.listen(0, '127.0.0.1', resolve);
      });
      const address = server.address();
      if (!address || typeof address === 'string')
        throw new Error('Expected a TCP server');
      process.env.WORKFLOW_LOCAL_BASE_URL = `http://127.0.0.1:${address.port}`;
    }, 120_000);

    afterEach(async () => {
      vi.restoreAllMocks();
      if (hostB) {
        const exited = once(hostB, 'exit');
        hostB.kill('SIGKILL');
        await exited;
        hostB = undefined;
      }
      await queueA?.close();
      queueA = undefined;
      await pool.query(
        'TRUNCATE graphile_worker._private_jobs, graphile_worker._private_job_queues, workflow.workflow_job_lock_holders'
      );
      hostBLines = [];
      deliveriesA = [];
      onDeliveryA = (response) => response.end('{}');
    });

    afterAll(async () => {
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
        server.closeAllConnections();
      });
      await pool.end();
      await container.stop();
    });

    test('a 1s-window peer releases a default-window delivery that then deletes the redelivery', async () => {
      const warn = vi.spyOn(console, 'warn');
      await startHostB(1);
      await runScenario(60);

      // A acknowledged after its stretch. If its fence held, the row would
      // still be there, locked by B. Instead it is gone while B still runs
      // the delivery.
      await expect
        .poll(jobRows, { timeout: 10_000, interval: 100 })
        .toEqual([]);
      expect(hostBLines).not.toContain('responded 500');
      expect(lostLockWarned(warn)).toBe(false);

      // B's delivery now fails. With the row intact Graphile would retry it;
      // here the message is lost: nothing is retried, by A or B.
      hostB!.stdin!.write('fail\n');
      await waitForHostB(/^responded 500$/);
      await new Promise((resolve) => setTimeout(resolve, 15_000));
      expect(await jobRows()).toEqual([]);
      expect(hostBLines.filter((line) => line.startsWith('claimed'))).toEqual([
        'claimed 2',
      ]);
      expect(deliveriesA).toEqual(['1']);
    }, 90_000);

    test('control: with equal windows the fence holds and the failure is retried', async () => {
      const warn = vi.spyOn(console, 'warn');
      await startHostB(2);
      await runScenario(2);

      // A's stretch outlasted its own confirm window, so it checked, found
      // the lock gone, and did not acknowledge: the row stays, held by B.
      await expect
        .poll(() => lostLockWarned(warn), { timeout: 10_000, interval: 100 })
        .toBe(true);
      const [heldByB] = await jobRows();
      expect(heldByB).toMatchObject({ attempts: 2 });
      expect(heldByB!.locked_by).not.toBeNull();

      // B's delivery fails, and Graphile retries it (attempt 3, on A or B).
      hostB!.stdin!.write('fail\n');
      await waitForHostB(/^responded 500$/);
      await expect
        .poll(
          () =>
            deliveriesA.includes('3') || hostBLines.includes('claimed 3'),
          { timeout: 30_000, interval: 200 }
        )
        .toBe(true);
    }, 90_000);
  }
);
