package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import cn.hutool.json.JSONUtil;

import javax.annotation.Resource;
import java.util.*;
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
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // 解决缓存穿透
        Shop shop = cacheClient
                .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 互斥锁解决缓存击穿
        // Shop shop = cacheClient
        //         .queryWithMutex(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 逻辑过期解决缓存击穿
        // Shop shop = cacheClient
        //         .queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        // 7.返回
        return Result.ok(shop);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.尝试从Redis缓存获取
        String cacheKey = CACHE_SHOP_TYPE_KEY + typeId + ":" + current;
        String json = stringRedisTemplate.opsForValue().get(cacheKey);
        if (json != null) {
            List<Shop> cachedShops = JSONUtil.toList(json, Shop.class);
            return Result.ok(cachedShops);
        }

        List<Shop> result;

        // 2.Redis缓存不存在，判断是否需要坐标查询
        if (x == null || y == null) {
            // 不需要坐标，直接数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            result = page.getRecords();
        } else {
            // 3.需要坐标查询，查Redis GEO
            int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
            int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

            String key = SHOP_GEO_KEY + typeId;
            GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                    .search(
                            key,
                            GeoReference.fromCoordinate(x, y),
                            new Distance(5000),
                            RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                    );

            // 4.GEO数据不存在，降级到数据库查询
            if (results == null || results.getContent() == null || results.getContent().isEmpty()) {
                Page<Shop> page = query()
                        .eq("type_id", typeId)
                        .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
                result = page.getRecords();
            } else {
                // 5.GEO数据存在，按距离排序查询
                List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
                if (list.size() <= from) {
                    return Result.ok(Collections.emptyList());
                }
                List<Long> ids = new ArrayList<>(list.size());
                Map<String, Distance> distanceMap = new HashMap<>(list.size());
                list.stream().skip(from).forEach(r -> {
                    String shopIdStr = r.getContent().getName();
                    ids.add(Long.valueOf(shopIdStr));
                    distanceMap.put(shopIdStr, r.getDistance());
                });
                String idStr = StrUtil.join(",", ids);
                List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
                for (Shop shop : shops) {
                    shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
                }
                result = shops;
            }
        }

        // 6.写入Redis缓存，TTL 30分钟
        if (result != null && !result.isEmpty()) {
            stringRedisTemplate.opsForValue().set(cacheKey, JSONUtil.toJsonStr(result), 30L, TimeUnit.MINUTES);
        }

        return Result.ok(result);
    }
}
