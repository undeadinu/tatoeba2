<?php
/**
 * AppShell file
 *
 * CakePHP(tm) : Rapid Development Framework (http://cakephp.org)
 * Copyright (c) Cake Software Foundation, Inc. (http://cakefoundation.org)
 *
 * Licensed under The MIT License
 * For full copyright and license information, please see the LICENSE.txt
 * Redistributions of files must retain the above copyright notice.
 *
 * @copyright     Copyright (c) Cake Software Foundation, Inc. (http://cakefoundation.org)
 * @link          http://cakephp.org CakePHP(tm) Project
 * @since         CakePHP(tm) v 2.0
 * @license       http://www.opensource.org/licenses/mit-license.php MIT License
 */

App::uses('Shell', 'Console');

/**
 * Application Shell
 *
 * Add your application-wide methods in the class below, your shells
 * will inherit them.
 *
 * @package       app.Console.Command
 */
class AppShell extends Shell {
    public $batchOperationSize = 1000;

    private function _orderCondition($nonUniqueField, $lastValue, $pKey, $lastId) {
        if ($nonUniqueField == $pKey) {
            return array("$pKey >" => $lastId);
        } else {
            return array('AND' => array(
                "$nonUniqueField >=" => $lastValue,
                array('OR' => array(
                    "$nonUniqueField >" => $lastValue,
                    array('AND' => array(
                        $nonUniqueField => $lastValue,
                        "$pKey >" => $lastId,
                    )),
                )),
            ));
        }
    }

    protected function batchOperation($model, $operation, $options) {
        assert(!isset($options['order']) || (is_string($options['order']) && strpos($options['order'], '.') !== false));

        $pKey = $this->{$model}->alias.'.'.$this->{$model}->primaryKey;
        $pKeyShort = $this->{$model}->primaryKey;

        if (!isset($options['order']) || (isset($options['order']) && $options['order'] == $pKeyShort)) {
            $options['order'] = $pKey;
        }
        $order = $options['order'];
        if ($order != $pKey) {
            $options['order'] = array($order, $pKey);
        }
        if (isset($options['fields'])) {
            $options['fields'][] = $order;
            $options['fields'][] = $pKey;
        }

        $proceeded = 0;
        $options = array_merge(
            array(
                'limit' => $this->batchOperationSize,
            ),
            $options
        );

        if (!isset($options['conditions'])) {
            $options['conditions'] = array();
        }
        $options['conditions'][] = array();
        end($options['conditions']);
        $conditionKey = key($options['conditions']);
        reset($options['conditions']);

        list($orderModel, $orderField) = explode('.', $order);
        $data = array();
        do {
            $data = $this->{$model}->find('all', $options);
            $args = func_get_args();
            array_splice($args, 0, 3, array($data, $model));
            $proceeded += call_user_func_array(array($this, $operation), $args);
            $lastRow = end($data);
            if ($lastRow) {
                $lastId = $lastRow[$model][$pKeyShort];
                $lastValue = $lastRow[$orderModel][$orderField];
                $options['conditions'][$conditionKey] = $this->_orderCondition($order, $lastValue, $pKey, $lastId);
            }
            echo ".";
        } while ($data);
        return $proceeded;
    }
}
