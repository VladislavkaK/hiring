import { IExecutor } from './Executor';
import ITask from './Task';

export default async function run(executor: IExecutor, queue: AsyncIterable<ITask>, maxThreads = 0) {
    maxThreads = Math.max(0, maxThreads);

    // Массив для хранения выполняющихся задач.
    const runningTasks: Promise<void>[] = [];

    // Объект для отслеживания выполняющихся задач по targetId.
    const taskMap: { [key: number]: Promise<void> } = {};

    // Вспомогательная функция, которая ждет, пока не освободится место для новой задачи.
    const waitForSlot = async () => {
        if (maxThreads > 0 && runningTasks.length >= maxThreads) {
            // Ждем завершения любой из выполняющихся задач.
            await Promise.race(runningTasks);
        }
    };

    // Асинхронный цикл по задачам из очереди.
    for await (const task of queue) {
        // Ждем, пока не освободится место для новой задачи.
        await waitForSlot();

        // Если задача с таким targetId уже выполняется, ждем её завершения.
        if (task.targetId in taskMap) {
            await taskMap[task.targetId];
        }

        // Создаем обещание для текущей задачи.
        const taskPromise = executor.executeTask(task).then(() => {
            // Когда задача завершена, удаляем её из runningTasks и taskMap.
            runningTasks.splice(runningTasks.indexOf(taskPromise), 1);
            delete taskMap[task.targetId];
        });

        // Добавляем новое обещание в массив выполняющихся задач и в taskMap.
        runningTasks.push(taskPromise);
        taskMap[task.targetId] = taskPromise;
    }

    // Ждем завершения всех оставшихся задач.
    await Promise.all(runningTasks);
}
