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
}
