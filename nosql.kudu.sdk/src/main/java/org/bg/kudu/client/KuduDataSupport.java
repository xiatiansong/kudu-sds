package org.bg.kudu.client;

/**
 * 具体使用KuduDataTemplate的类来继承此类
 *
 * @author xiatiansong
 */
public abstract class KuduDataSupport {

    private KuduDataTemplate kuduDataTemplate;

    /**
     * @param kuduDataTemplate the kuduDataTemplate to set
     */
    public void setKuduDataTemplate(KuduDataTemplate kuduDataTemplate) {
        this.kuduDataTemplate = kuduDataTemplate;
    }

    /**
     * @return the kuduDataTemplate
     */
    public KuduDataTemplate getKuduDataTemplate() {
        return kuduDataTemplate;
    }
}