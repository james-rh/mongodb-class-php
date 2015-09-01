<?php
/**
 * Mongodb简单封装
 * james add 20140626
 */

class Mymongo
{
    private $mongo, $dbname, $mongo_connect, $mdb, $table;

    /**
     * 初始化
     */
    public function __construct()
    {
        $ci =& get_instance();
        $ci->load->config('mongo');
        $linkstr = $ci->config->item('linkstr');
        $dbname = $ci->config->item('dbname');
        $readPreference = $ci->config->item('readPreference');
        if (empty($linkstr) || empty($dbname))
        {
            return false;
        }
        empty($readPreference) && $readPreference = "secondaryPreferred";

      //  $option = array('connect' => false, 'readPreference' => $readPreference);// MongoClient::RP_SECONDARY_PREFERRED
	    $option = array( 'readPreference' => $readPreference);

        $this->dbname = $dbname;
        $this->mongo_connect = false;
        $this->mongo = new MongoClient($linkstr, $option);
    }

    /**
     * 关闭
     */
    public function __destruct()
    {
        $this->close();
    }

    public function close()
    {
        if ($this->mongo_connect)
        {
            $this->mongo->close();
            $this->mongo_connect = false;
            unset($this->mdb);
            unset($this->table);
        }
    }

    /**
     * 查询一条记录
     * @param string $collection_name 集合名称(相当于关系数据库中的表)
     * @param array $where 查询的条件array(key=>value) 相当于key=value
     * @param array $filed 需要列表的字段信息array(filed1,filed2)
     * @param array $sort 需要列表的字段信息array(filed1=>1) -1降序
     */
    public function find_one($collection_name, $where, $filed = array())
    {
        $this->_auto_connection_mongondb($collection_name);
        if (!$this->mongo_connect) return false;
        $result = $this->table[$collection_name]->findOne($where, $filed);
        return $result;
    }

    private function _auto_connection_mongondb($collection_name)
    {
        if (!$this->mongo_connect) $this->connect();
        if (!$this->table[$collection_name] and $this->mongo_connect) $this->select_collection($collection_name);
        return $this->mongo_connect;
    }

    public function connect($dbname = '')
    {
        !empty($dbname) && $this->dbname = $dbname;
        try
        {
	        if ($this->mongo->connect())
            {
                $this->select_db($this->dbname);
                $this->mongo_connect = true;
                return true;
            }

        } catch (Exception $e)
        {
        }
        $this->mongo_connect = false;
        return false;
    }

    public function select_db($db_name)
    {
        $this->mdb = $this->mongo->selectDB($db_name);
    }

    public function select_collection($collection_name)
    {
        $this->table[$collection_name] = $this->mdb->selectCollection($collection_name);
    }

    public function find($collection_name, $where, $field = array(), $sort = array(), $skip = 0, $limit = 0)
    {
        $this->_auto_connection_mongondb($collection_name);
        if (!$this->mongo_connect) return false;
        $result = array();
        $cursor = $this->table[$collection_name]->find($where, $field);
        if ($sort) $cursor = $cursor->sort($sort);
        if ($skip > 0 or $limit > 0) $cursor = $cursor->skip($skip)->limit($limit);

        //$result=iterator_to_array($cursor);
        while ($cursor->hasNext())
        {
            $result[] = $cursor->getNext();
        }
        return $result;
    }

    /**
     * 查询记录集的条数
     * @param string $collection_name 集合名称(相当于关系数据库中的表)
     * @param array $where
     * @return int
     */
    public function count($collection_name, $where = array())
    {
        $this->_auto_connection_mongondb($collection_name);
        if (!$this->mongo_connect) return false;
        return $this->table[$collection_name]->count($where);
    }

    /**
     * 插入数据
     * @param string $collection_name 集合名称(相当于关系数据库中的表)
     * @param array $data_array
     * @param array $options array('safe'=>true,'fsync'=>true,'timeout'=>1);
     * safe 保障数据安全性写入？默认false，更快速
     * fsync 保障数据硬盘写入?
     * timeout 当safe为true时，客户端等待时间
     * 操作完成后 data_array 会多出_id
     * save 不存在_id时则insert,存在_id时则update
     */
    public function insert($collection_name, $data_array, $options = array())
    {
        $this->_auto_connection_mongondb($collection_name);
        if (!$this->mongo_connect) return false;
        return $this->table[$collection_name]->insert($data_array, $options);
    }

    public function save($collection_name, $data_array, $options = array())
    {
        $this->_auto_connection_mongondb($collection_name);
        if (!$this->mongo_connect) return false;
        return $this->table[$collection_name]->save($data_array, $options);
    }

    /**
     * 更新数据(注一次只能更新一条记录)
     * @param string $collection_name 集合名称|表名
     * @param array $where 查询条件array(key=>value)
     * @param array $update_data 要更新的数据
     * @param array $options array('upsert'=>true,'multiple'=>true,'safe'=>true,'fsync'=>true,'timeout'=>1);
     * upsert 条件不存在时是否插入
     * multiple 修改全部 还是只修改单条，默认为false只改一条数据
     * @return bool
     */
    public function update($collection_name, $where, $update_data, $options = array('multiple' => true))
    {
        $this->_auto_connection_mongondb($collection_name);
        if (!$this->mongo_connect) return false;
        $result = $this->table[$collection_name]->update($where, $update_data, $options);
        return $result;
    }

    /**
     * 删除记录
     * @param string $collection_name 集合名称(相当于关系数据库中的表)
     * @param array $where 删除条件
     * @param array $options array('justOne'=>true,'safe'=>true,'fsync'=>true,'timeout'=>1);
     * justOne 只删除一条数据
     * @return unknown
     */
    public function delete($collection_name, $where, $option = array("justOne" => false))
    {
        return $this->remove($collection_name, $where, $option);
    }

    public function remove($collection_name, $where, $option = array("justOne" => false))
    {
        $this->_auto_connection_mongondb($collection_name);
        if (!$this->mongo_connect) return false;
        $result = $this->table[$collection_name]->remove($where, $option);
        return $result;
    }

}

?>
