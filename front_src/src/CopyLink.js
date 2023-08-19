import { Button, Modal, Input, Space, Popover } from 'antd';
import React, { useState } from 'react';
import { LinkOutlined } from '@ant-design/icons';

const CopyLink = (props) => {
    const [isModalOpen, setIsModalOpen] = useState(false);
    const showModal = () => {
        setIsModalOpen(true);
    };
    const handleOk = () => {
        setIsModalOpen(false);
    };
    const handleCancel = () => {
        setIsModalOpen(false);
    };
    return (
        <>
            <Button onClick={showModal}>Просмотр ссылки <LinkOutlined /></Button>
            <Modal title="Ссылка события" open={isModalOpen} onOk={handleOk} onCancel={handleCancel}>
                <Space direction='horizontal'>

                    <Input style={{ width: 350 }} value={props.link} />

                    <Popover content="Ссылка скопирована в буфер обмена" trigger="click">
                        <Button onClick={() => navigator.clipboard.writeText(props.link)} >Скопировать</Button>
                    </Popover>
                </Space>
            </Modal>
        </>
    );
};
export default CopyLink;