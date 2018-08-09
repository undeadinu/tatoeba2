<?php
App::uses('ConsoleOutput', 'Console');
App::uses('ConsoleInput', 'Console');
App::uses('QueueExportTask', 'Console/Command/Task');

class QueueExportTaskTest extends CakeTestCase
{
    public $fixtures = array(
        'app.sentence',
        'app.contribution',
        'app.reindex_flag',
        'app.link',
        'app.user',
    );

    public function setUp()
    {
        parent::setUp();
        $out = $this->getMock('ConsoleOutput', array(), array(), '', false);
        $in = $this->getMock('ConsoleInput', array(), array(), '', false);

        $this->QueueExportTask = $this->getMock(
            'QueueExportTask',
            array('in', 'err', 'createFile', '_stop', 'clear'),
            array($out, $out, $in)
        );
        $this->QueueExportTask->batchOperationSize = 10;

        $this->testExportDir = TMP.'testExportDir';
        $this->removeDirRecursively($this->testExportDir);
        mkdir($this->testExportDir);
        if (!is_dir($this->testExportDir)) {
            die("Couldn't create test directory '{$this->testExportDir}'");
        }
        chmod($this->testExportDir, 0777);
    }

    private function removeDirRecursively($dir)
    {
        if (is_dir($dir)) {
            $objects = scandir($dir);
            foreach ($objects as $object) {
                if ($object != '.' && $object != '..') {
                    if (filetype($dir.'/'.$object) == 'dir') {
                        $this->removeDirRecursively($dir.'/'.$object);
                    } else {
                        unlink($dir.'/'.$object);
                    }
                }
            }
            reset($objects);
            rmdir($dir);
        }
    }

    public function tearDown()
    {
        parent::tearDown();
        unset($this->QueueExportTask);
        $this->removeDirRecursively($this->testExportDir);
    }

    private function manyWeirdChars()
    {
        $str = 'ASCII:';
        for ($i = 0; $i < 255; $i++) {
            $str .= chr($i);
        }
        return $str;
    }

    public function testExportData()
    {
        $expected = $this->testExportDir.DS.'expected.csv';
        $text = $this->manyWeirdChars();
        // Bypass the model layer to prevent $text from being cleaned
        $db = $this->QueueExportTask->Sentence->getDataSource();
        $db->create(
            $this->QueueExportTask->Sentence,
            array('lang', 'user_id', 'text'),
            array('eng', 7, $text)
        );

        $this->QueueExportTask->Sentence->query(
            "SELECT id, lang, text FROM `sentences` "
           ."WHERE correctness > -1 "
           ."INTO OUTFILE '$expected'"
        );

        $actual = $this->testExportDir.DS.'sentences.csv';
        $options = array(
            'exportDir' => $this->testExportDir,
        );
        $this->QueueExportTask->exportData(
            $actual,
            'Sentence',
            array(
                'fields' => array('id', 'lang', 'text'),
                'conditions' => array('correctness >' => -1),
            )
        );

        $this->assertEquals(sha1_file($expected), sha1_file($actual));
    }

    public function testExportDataWithJoin()
    {
        $expected = $this->testExportDir.DS.'expected.csv';
        $this->QueueExportTask->Sentence->query(
            "SELECT s.id, s.lang, s.text, u.username, s.created, s.modified "
           ."FROM `sentences` s LEFT JOIN `users` u ON s.user_id = u.id "
           ."WHERE correctness > -1 "
           ."INTO OUTFILE '$expected'"
        );

        $actual = $this->testExportDir.DS.'sentences_detailed.csv';
        $options = array(
            'exportDir' => $this->testExportDir,
        );
        $this->QueueExportTask->exportData(
            $actual,
            'Sentence',
            array(
                'fields' => array('id', 'lang', 'text', 'User.username', 'created', 'modified'),
                'conditions' => array('correctness >' => -1),
                'contain' => array('User'),
            )
        );

        $this->assertEquals(sha1_file($expected), sha1_file($actual));
    }

    public function testExportDataWithoutPrimaryKey()
    {
        $expected = $this->testExportDir.DS.'expected.csv';
        $this->QueueExportTask->Sentence->query(
            "SELECT sentence_id, translation_id FROM `sentences_translations` "
           ."INTO OUTFILE '$expected'"
        );

        $actual = $this->testExportDir.DS.'links.csv';
        $options = array(
            'exportDir' => $this->testExportDir,
        );
        $this->QueueExportTask->exportData(
            $actual,
            'Link',
            array(
                'fields' => array('sentence_id', 'translation_id'),
                'order' => 'Link.sentence_id',
            )
        );

        $this->assertEquals(sha1_file($expected), sha1_file($actual));
    }

    public function testExportDataWithoutOrderingOnPrimaryKey()
    {
        $this->QueueExportTask->batchOperationSize = 1;
        $expected = $this->testExportDir.DS.'expected.csv';
        $this->QueueExportTask->Sentence->query(
            "SELECT u.username, c.datetime, c.action, c.type, c.sentence_id, "
           ."c.sentence_lang, c.translation_id, c.text "
           ."FROM contributions c LEFT JOIN users u ON c.user_id = u.id "
           ."ORDER BY c.datetime ASC "
           ."INTO OUTFILE '$expected'"
        );

        $actual = $this->testExportDir.DS.'contributions.csv';
        $options = array(
            'exportDir' => $this->testExportDir,
        );
        $this->QueueExportTask->exportData(
            $actual,
            'Contribution',
            array(
                'fields' => array('User.username', 'datetime', 'action', 'type', 'sentence_id', 'sentence_lang', 'translation_id', 'text'),
                'contain' => array('User'),
                'order' => 'Contribution.datetime',
            )
        );

        $this->assertEquals(sha1_file($expected), sha1_file($actual));
    }

    public function testCompressFile()
    {
        $file = $this->testExportDir.DS.'export.csv';
        $contents = "Some data.\nAnother line.";
        file_put_contents($file, $contents);

        $expectedFile = $this->testExportDir.DS.'export.tar.bz2';
        $expectedIndex = array(basename($file));
        $expectedContents = explode("\n", $contents);

        $this->QueueExportTask->compressFile($file);

        $this->assertFileExists($expectedFile);

        exec("tar tf $expectedFile", $archiveIndex);
        $this->assertEquals($expectedIndex, $archiveIndex);

        exec("tar Oxf $expectedFile", $archiveContents);
        $this->assertEquals($expectedContents, $archiveContents);
    }

    public function testRun()
    {
        $options = array(
            'exportDir' => $this->testExportDir,
            'exports' => array(
                'sentences.csv' => array(
                    'model' => 'Sentence',
                    'findOptions' => array(
                        'fields' => array('id', 'lang', 'text'),
                        'conditions' => array('correctness >' => -1),
                    ),
                ),
            ),
        );
        $expectedArchive = $this->testExportDir.DS.'sentences.tar.bz2';
        $expectedCSV = $this->testExportDir.DS.'sentences.csv';

        $this->QueueExportTask->run($options);

        $this->assertFileExists($expectedArchive);
        $this->assertFileExists($expectedCSV);
    }

    public function testRunRemovesCSV()
    {
        $options = array(
            'exportDir' => $this->testExportDir,
            'exports' => array(
                'text.csv' => array(
                    'model' => 'Sentence',
                    'findOptions' => array(
                        'fields' => array('id', 'text'),
                    ),
                    'remove_csv_file' => true,
                ),
            ),
        );
        $expectedArchive = $this->testExportDir.DS.'text.tar.bz2';
        $notExpectedCSV = $this->testExportDir.DS.'text.csv';

        $this->QueueExportTask->run($options);

        $this->assertFileExists($expectedArchive);
        $this->assertFileNotExists($notExpectedCSV);
    }
}
