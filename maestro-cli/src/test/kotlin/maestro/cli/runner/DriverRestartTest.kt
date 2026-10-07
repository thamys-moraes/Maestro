package maestro.cli.runner

import com.google.common.truth.Truth.assertThat
import org.junit.jupiter.api.Test

class DriverRestartTest {

    private fun run(
        outcomes: List<WorkerOutcome>,
        maxRestarts: Int = 2,
        shouldContinue: () -> Boolean = { true },
    ): Pair<WorkerOutcome, List<Boolean>> {
        val calls = mutableListOf<Boolean>()
        val result = runWithDriverRestarts(
            maxRestarts = maxRestarts,
            shouldContinue = shouldContinue,
            onRestart = {},
        ) { isRestart ->
            calls.add(isRestart)
            outcomes[calls.size - 1]
        }
        return result to calls
    }

    @Test
    fun `finished worker does not restart`() {
        val (outcome, calls) = run(listOf(WorkerOutcome.FINISHED))

        assertThat(outcome).isEqualTo(WorkerOutcome.FINISHED)
        assertThat(calls).containsExactly(false)
    }

    @Test
    fun `crashed worker restarts the driver and keeps consuming the queue`() {
        val (outcome, calls) = run(listOf(WorkerOutcome.CRASHED, WorkerOutcome.FINISHED))

        assertThat(outcome).isEqualTo(WorkerOutcome.FINISHED)
        assertThat(calls).containsExactly(false, true).inOrder()
    }

    @Test
    fun `worker gives up after max restarts`() {
        val (outcome, calls) = run(List(3) { WorkerOutcome.CRASHED }, maxRestarts = 2)

        assertThat(outcome).isEqualTo(WorkerOutcome.CRASHED)
        assertThat(calls).containsExactly(false, true, true).inOrder()
    }

    @Test
    fun `zero max restarts keeps the previous behaviour`() {
        val (outcome, calls) = run(listOf(WorkerOutcome.CRASHED), maxRestarts = 0)

        assertThat(outcome).isEqualTo(WorkerOutcome.CRASHED)
        assertThat(calls).containsExactly(false)
    }

    @Test
    fun `no restart when the queue is already drained`() {
        val (outcome, calls) = run(listOf(WorkerOutcome.CRASHED), shouldContinue = { false })

        assertThat(outcome).isEqualTo(WorkerOutcome.FINISHED)
        assertThat(calls).containsExactly(false)
    }

    @Test
    fun `failure to reopen the session propagates so the device is dropped`() {
        var calls = 0
        val error = runCatching {
            runWithDriverRestarts(maxRestarts = 2, shouldContinue = { true }, onRestart = {}) { isRestart ->
                calls++
                if (isRestart) throw IllegalStateException("device offline")
                WorkerOutcome.CRASHED
            }
        }.exceptionOrNull()

        assertThat(error).hasMessageThat().isEqualTo("device offline")
        assertThat(calls).isEqualTo(2)
    }
}
