<?php

App::uses('QueueTask', 'Queue.Console/Command/Task');

class QueueExportTask extends QueueTask {

    public $uses = array(
        'Sentence',
    );

    private $weeklyExports = array(
        'sentences.csv' => array(
            'model' => 'Sentence',
            'findOptions' => array(
                'fields' => array('id', 'lang', 'text'),
                'conditions' => array('correctness >' => -1),
            ),
        ),
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
        $exportDir = isset($this->args[1]) ? $this->args[1] : '';
        if (!is_dir($exportDir)) {
            if (!empty($exportDir)) {
                $this->out("Error: '$exportDir' is not a directory.");
            }
            $this->out('Usage: cake Queue.Queue add Export <export-dir>');
            return;
        }

        $exports = $this->weeklyExports;
        $options = array(
            'exportDir' => $exportDir,
            'exports' => $this->weeklyExports,
        );
        if ($this->QueuedTask->createJob('Export', $options)) {
            $this->out('OK, job created, now run the worker');
        } else {
            $this->err('Could not create Job');
        }
    }

    public function compressFile($file) {
        $archive = substr($file, 0, strrpos($file, '.')).'.tar.bz2';
        $descriptorSpec = array(
           0 => array('pipe', 'r'),
           1 => array('file', $archive, 'w'),
           2 => array('pipe', 'w'),
        );
        $cwd = dirname($file);
        $process = proc_open('tar -T - -jc', $descriptorSpec, $pipes, $cwd);
        if (is_resource($process)) {
            fwrite($pipes[0], basename($file));
            fclose($pipes[0]);
            do {
                usleep(500000);
                $status = proc_get_status($process);
            } while (is_array($status) && $status['running']);
            fclose($pipes[2]);
            proc_close($process);
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
        $dataSource = $this->Sentence->getDataSource();
        $dataSource->begin();

        foreach ($data['exports'] as $filename => $export) {
            $completeFilename = $data['exportDir'].DS.$filename;
            $ok = $this->exportData(
                $completeFilename,
                $export['model'],
                $export['findOptions']
            );
            if ($ok) {
                $this->compressFile($completeFilename);
            }
        }

        $dataSource->commit();
        return $ok;
    }

    protected function exportRows($rows, $modelName, $fp) {
        foreach ($rows as $row) {
            $this->fputcsvLikeMySQL($fp, $row[$modelName]);
        }
    }

    public function exportData($outFile, $modelName, $findOptions) {
        $fp = fopen($outFile, 'w');
        $proceeded = $this->batchOperation(
            $modelName,
            'exportRows',
            $findOptions,
            $fp
        );
        fclose($fp);
        $this->out(' ');
        $this->out("Wrote $outFile");
        return true;
    }
}
