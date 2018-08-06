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
    }

    public function tearDown()
    {
        parent::tearDown();
        unset($this->QueueExportTask);
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
        $expected = TMP.DS.'expected.csv';
        $text = $this->manyWeirdChars();
        // Bypass the model layer to prevent $text from being cleaned
        $db = $this->QueueExportTask->Sentence->getDataSource();
        $db->create(
            $this->QueueExportTask->Sentence,
            array('lang', 'user_id', 'text'),
            array('eng', 7, $text)
        );

        @unlink($expected);
        $this->QueueExportTask->Sentence->query(
            "SELECT id, lang, text FROM `sentences` "
           ."WHERE correctness > -1 "
           ."INTO OUTFILE '$expected'"
        );

        $actual = TMP.DS.'sentences.csv';
        $options = array(
            'exportDir' => TMP,
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
        @unlink($expected);
        @unlink($actual);
    }

    public function testCompressFile()
    {
        $file = TMP.DS.'export.csv';
        $contents = "Some data.\nAnother line.";
        file_put_contents($file, $contents);

        $expectedFile = TMP.DS.'export.tar.bz2';
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
            'exportDir' => TMP,
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
        $expectedFile = TMP.DS.'sentences.tar.bz2';

        $this->QueueExportTask->run($options);

        $this->assertFileExists($expectedFile);
    }
}
