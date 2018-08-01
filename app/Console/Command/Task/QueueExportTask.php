<?php

App::uses('QueueTask', 'Queue.Console/Command/Task');

class QueueExportTask extends QueueTask {

    public $uses = array(
        'Sentence',
    );

/**
 * ZendStudio Codecomplete Hint
 *
 * @var QueuedTask
 */
    public $QueuedTask;

/**
 * Timeout for run, after which the Task is reassigned to a new worker.
 *
 * @var int
 */
    public $timeout = 10;

/**
 * Number of times a failed instance of this task should be restarted before giving up.
 *
 * @var int
 */
    public $retries = 1;

/**
 * Stores any failure messages triggered during run()
 *
 * @var string
 */
    public $failureMessage = '';

/**
 * Example add functionality.
 * Will create one example job in the queue, which later will be executed using run();
 *
 * @return void
 */
    public function add() {
        $this->out('Tatoeba exports task.');
        $this->hr();
        $this->out('This task generates the daily CSV exports in XXX.');
        $this->out(' ');
        $this->out('To run a Worker use:');
        $this->out('    cake Queue.Queue runworker');
        $this->out(' ');
        $this->out('You can find the sourcecode of this task in: ');
        $this->out(__FILE__);
        $this->out(' ');
        if ($this->QueuedTask->createJob('Export', null)) {
            $this->out('OK, job created, now run the worker');
        } else {
            $this->err('Could not create Job');
        }
    }

    private function fputcsvLikeMySQL($fh, $fields) {
        foreach ($fields as &$field) {
            if (is_null($field)) {
                $field = '\N';
            } else {
                $field = preg_replace('/[\n\t\\\\]/u', '\\\\$0', $field);
                $field = str_replace("\x00", '\\0', $field);
            }
        }
        fputs($fh, implode($fields, "\t")."\n");
    }

/**
 * Example run function.
 * This function is executed, when a worker is executing a task.
 * The return parameter will determine, if the task will be marked completed, or be requeued.
 *
 * @param array $data The array passed to QueuedTask->createJob()
 * @param int $id The id of the QueuedTask
 * @return bool Success
 */
    public function run($data, $id = null) {
        $outFile = TMP.DS.'sentences.csv';
        $data = $this->Sentence->find('all', array(
            'fields' => array('id', 'lang', 'text'),
            'conditions' => array('correctness >' => -1)
        ));
        $fp = fopen($outFile, 'w');
        foreach ($data as $row) {
            $this->fputcsvLikeMySQL($fp, $row[$this->Sentence->alias]);
        }
        fclose($fp);
        $this->out(' ');
        $this->out("Wrote $outFile");
        return true;
    }
}