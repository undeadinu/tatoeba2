<?php

App::uses('QueueTask', 'Queue.Console/Command/Task');

class QueueExportTask extends QueueTask {

    public $uses = array(
        'Sentence',
        'Link',
        'Contribution',
        'SentenceComment',
        'Wall',
        'Tag',
        'SentencesList',
        'SentencesSentencesLists',
        'SentenceAnnotation',
        'Audio',
        'UsersLanguages',
        'TagsSentences',
    );

    private $weeklyExports = array(
        'sentences_detailed.csv' => array(
            'model' => 'Sentence',
            'findOptions' => array(
                'fields' => array('id', 'lang', 'text', 'User.username', 'Sentence.created', 'modified'),
                'conditions' => array('correctness >' => -1),
                'contain' => array('User'),
            ),
        ),
        'sentences.csv' => array(
            'model' => 'Sentence',
            'findOptions' => array(
                'fields' => array('id', 'lang', 'text'),
                'conditions' => array('correctness >' => -1),
            ),
        ),
        'links.csv' => array(
            'model' => 'Link',
            'findOptions' => array(
                'fields' => array('sentence_id', 'translation_id'),
                'order' => array('Link.sentence_id', 'Link.id'),
            ),
        ),
        'contributions.csv' => array(
            'model' => 'Contribution',
            'findOptions' => array(
                'fields' => array('User.username', 'datetime', 'action', 'type', 'sentence_id', 'sentence_lang', 'translation_id', 'text'),
                'contain' => array('User'),
                'order' => array('Contribution.datetime', 'Contribution.id'),
            ),
            'remove_csv_file' => true,
        ),
        'sentence_comments.csv' => array(
            'model' => 'SentenceComment',
            'findOptions' => array(
                'fields' => array('id', 'sentence_id', 'User.username', 'created', 'text'),
                'contain' => array('User'),
                'order' => array('SentenceComment.created', 'SentenceComment.id'),
            ),
            'remove_csv_file' => true,
            'archive_name' => 'comments.tar.bz2',
        ),
        'wall_posts.csv' => array(
            'model' => 'Wall',
            'findOptions' => array(
                'fields' => array('id', 'User.username', 'parent_id', 'date', 'content'),
                'contain' => array('User'),
                'order' => 'Wall.id',
            ),
            'remove_csv_file' => true,
            'archive_name' => 'wall.tar.bz2',
        ),
        'tags.csv' => array(
            'model' => 'Tag',
            'findOptions' => array(
                'fields' => array('DISTINCT TagsSentences.sentence_id', 'Tag.name'),
                'joins' => array(array(
                    'table' => 'tags_sentences',
                    'alias' => 'TagsSentences',
                    'conditions' => array('Tag.id = TagsSentences.tag_id'),
                )),
                'order' => array('TagsSentences.sentence_id', 'Tag.name'),
            ),
        ),
        'user_lists.csv' => array(
            'model' => 'SentencesList',
            'findOptions' => array(
                'fields' => array('id', 'User.username', 'created', 'modified', 'name', 'editable_by'),
                'contain' => array('User'),
                'conditions' => array('NOT' => array('visibility' => 'private')),
            ),
        ),
        'sentences_in_lists.csv' => array(
            'model' => 'SentencesSentencesLists',
            'findOptions' => array(
                'fields' => array('SentencesList.id', 'sentence_id'),
                'contain' => array('SentencesList'),
                'conditions' => array('NOT' => array('SentencesList.visibility' => 'private')),
                'order' => array('SentencesList.id', 'SentencesSentencesLists.sentence_id')
            ),
        ),
        'jpn_indices.csv' => array(
            'model' => 'SentenceAnnotation',
            'findOptions' => array(
                'fields' => array('sentence_id', 'meaning_id', 'text'),
            ),
        ),
        'sentences_with_audio.csv' => array(
            'model' => 'Audio',
            'findOptions' => array(
                'fields' => array('sentence_id', 'User.username', 'User.audio_license', 'User.audio_attribution_url'),
                'contain' => array('User'),
                'order' => array('Audio.sentence_id', 'Audio.id'),
            ),
        ),
        'user_languages.csv' => array(
            'model' => 'UsersLanguages',
            'findOptions' => array(
                'fields' => array('language_code', 'level', 'User.username', 'details'),
                'contain' => array('User'),
                'order' => array('UsersLanguages.language_code', 'UsersLanguages.id'),
            ),
        ),
        'tags_detailed.csv' => array(
            'model' => 'TagsSentences',
            'findOptions' => array(
                'fields' => array('tag_id', 'sentence_id', 'User.username', 'added_time'),
                'contain' => array('User'),
                'order' => array('TagsSentences.added_time', 'TagsSentences.id'),
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

    public function compressFile($file, $archive = null) {
        if (is_null($archive)) {
            $archive = substr($file, 0, strrpos($file, '.')).'.tar.bz2';
        }
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
                $archiveName = isset($export['archive_name']) ? $data['exportDir'].DS.$export['archive_name'] : null;
                $this->compressFile($completeFilename, $archiveName);
            }
            if (isset($export['remove_csv_file']) && $export['remove_csv_file']) {
                unlink($completeFilename);
            }
        }

        $dataSource->commit();
        return $ok;
    }

    private function calculateFieldsMap($row, $modelName, $fields) {
        $map = array();
        foreach ($fields as $cakeField) {
            $cakeField = str_ireplace('distinct', '', $cakeField);
            $cakeField = trim($cakeField);
            $parts = explode('.', $cakeField, 2);
            if (count($parts) == 2) {
                list($model, $field) = $parts;
            } else {
                $model = $modelName;
                $field = $parts[0];
            }
            $map[] = array($model, $field);
        }
        return $map;
    }

    protected function exportRows($rows, $modelName, $fp, $fields) {
        if (count($rows) == 0) {
            return;
        }

        $fieldsMap = $this->calculateFieldsMap($rows[0], $modelName, $fields);
        foreach ($rows as $row) {
            $sortedRow = array_map(
                function ($map) use ($row) {
                    return $row[ $map[0] ][ $map[1] ];
                },
                $fieldsMap
            );
            $this->fputcsvLikeMySQL($fp, $sortedRow);
        }
    }

    public function exportData($outFile, $modelName, $findOptions) {
        $fp = fopen($outFile, 'w');
        $proceeded = $this->batchOperation(
            $modelName,
            'exportRows',
            $findOptions,
            $fp,
            $findOptions['fields']
        );
        fclose($fp);
        $this->out(' ');
        $this->out("Wrote $outFile");
        return true;
    }
}
