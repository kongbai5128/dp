package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static com.hmdp.utils.RedisConstants.*;
/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        String key = CACHE_SHOP_TYPE_KEY + "list";
        // 1.从Redis查询
        String json = stringRedisTemplate.opsForValue().get(key);
        if (json != null) {
            List<ShopType> list = JSONUtil.toList(json, ShopType.class);
            return Result.ok(list);
        }
        // 2.Redis不存在，查询数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();
        // 3.写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(typeList), 30L, TimeUnit.MINUTES);
        return Result.ok(typeList);
    }
}

