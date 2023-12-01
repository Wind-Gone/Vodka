package bean;

public class ReservoirSamplingSingleton {

    // 私有构造函数，防止直接实例化
    private ReservoirSamplingSingleton() {
    }

    private static final class InstanceHolder {
        private static final ReservoirSampling instance = new ReservoirSampling(400000);
    }

    // 获取单例实例
    public static ReservoirSampling getInstance() {
        return InstanceHolder.instance;
    }
}
