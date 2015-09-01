<?php
class Myredis
{
    private $redis;

    public function __construct()
    {
        $ci =& get_instance();
        $ci->load->config('redis');
        $server = $ci->config->item('host1');
        $port = $ci->config->item('port1');
        $timeout = $ci->config->item('timeout');

        $this->redis = new Redis();
        try
        {
            $this->redis->connect($server, $port);
        } catch (Exception $e)
        {
        }
    }

    public function set($key, $value, $time_out = 0)
    {
        $retRes = $this->redis->set($key, $value);
        if ($time_out > 0)
        {
            $this->redis->setTimeout($key, $time_out);
        }
        return $retRes;
    }

    public function get($key)
    {
        $value = $this->redis->get($key);
        return $this->_decode($value);
    }

    public function delete($key)
    {
        return $this->redis->delete($key);
    }

    public function flushAll()
    {
        return $this->redis->flushAll();
    }

    public function push($key, $value, $right = true)
    {
        return $right ? $this->redis->rPush($key, $value) : $this->redis->lPush($key, $value);
    }

    public function pop($key, $left = true)
    {
        return $left ? $this->redis->lPop($key) : $this->redis->rPop($key);
    }

    public function increment($key)
    {
        return $this->redis->incr($key);
    }

    public function decrement($key)
    {
        return $this->redis->decr($key);
    }

    public function exists($key)
    {
        return $this->redis->exists($key);
    }

    public function hset($key, $field, $value)
    {
        return $this->redis->hset($key, $field, $value);
    }

    public function hget($key, $field)
    {
        $value = $this->redis->hget($key, $field);
        return $this->_decode($value);
    }

    public function hkeys($key)
    {
        $value = $this->redis->hkeys($key);
        return $this->_decode($value);
    }

    public function hgetall($key)
    {
        $value = $this->redis->hgetall($key);
        return $this->_decode($value);
    }

    private function _encode($value)
    {
        return json_encode($value, JSON_UNESCAPED_UNICODE);
    }

    private function _decode($value)
    {
        return json_decode($value, true);
    }
}

?>
